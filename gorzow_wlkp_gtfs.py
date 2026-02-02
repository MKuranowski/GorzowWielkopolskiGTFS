# SPDX-FileCopyrightText: 2025-2026 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import argparse
import csv
import io
import logging
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from html.parser import HTMLParser
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from urllib.parse import urljoin, urlparse
from zipfile import ZIP_DEFLATED, ZipFile

import requests
from impuls import App, PipelineOptions, Task, TaskRuntime
from impuls.errors import InputNotModified
from impuls.pipeline import Pipeline
from impuls.resource import DATETIME_MIN_UTC, ConcreteResource
from impuls.tasks import (
    ExecuteSQL,
    ExtendCalendarsFromPolishExceptions,
    LoadGTFS,
    SaveGTFS,
    SimplifyCalendars,
)
from impuls.tools import polish_calendar_exceptions

LIST_URL = "https://bip.mzk-gorzow.com.pl/gtfs.html"
USER_AGENT = "Mozilla/5.0 (compatible; MSIE 7.0; Windows 95; Trident/3.0)"

GTFS_HEADERS = {
    "agency.txt": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "feed_info.txt": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_start_date",
        "feed_end_date",
    ),
    "stops.txt": ("stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon"),
    "routes.txt": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "trips.txt": ("route_id", "service_id", "trip_id", "direction_id", "block_id", "shape_id"),
    "stop_times.txt": ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
    "calendar_dates.txt": ("date", "service_id", "exception_type"),
    "shapes.txt": ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
}


class LinkScraper(HTMLParser):
    def __init__(self, *, base_url: str, convert_charrefs: bool = True) -> None:
        super().__init__(convert_charrefs=convert_charrefs)
        self.base_url = base_url
        self.latest_gtfs_link: str = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # Ignore non-anchor
        if tag != "a":
            return

        # Extract the anchor reference
        href = ""
        for k, v in attrs:
            if k == "href" and v:
                href = v

        # Ignore link to non-gtfs files
        if "/download/" not in href or "gtfs" not in href or "zip" not in href:
            return

        # Save the link
        self.latest_gtfs_link = urljoin(self.base_url, href)

    @classmethod
    def find_latest(cls, url: str = LIST_URL, ua: str = USER_AGENT) -> str:
        with requests.get(url, headers={"User-Agent": ua}, stream=True) as r:
            r.raise_for_status()
            self = cls(base_url=r.url)
            for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                self.feed(chunk)
        if not self.latest_gtfs_link:
            raise ValueError(f"no link to GTFS_GW.zip extracted from {url}")
        return self.latest_gtfs_link


class GorzowGTFSResource(ConcreteResource):
    url: str

    def __init__(self) -> None:
        super().__init__()
        self.url = ""

    def save_extra_metadata(self) -> dict[str, Any] | None:
        return {"url": self.url}

    def load_extra_metadata(self, metadata: dict[str, Any]) -> None:
        self.url = self.url or metadata.get("url", "")

    def fetch(self, conditional: bool) -> Iterator[bytes]:
        # Scrape for the latest file URL
        latest_url = LinkScraper.find_latest()
        if latest_url != self.url:
            # Different url - force download
            self.url = latest_url
            conditional = False

        # Append the If-Modified-Since header
        # NOTE: The server doesn't return ETag, so no need to remember that
        headers = {"User-Agent": USER_AGENT}
        if conditional and self.last_modified != DATETIME_MIN_UTC:
            headers["If-Modified-Since"] = format_datetime(
                self.last_modified.astimezone(timezone.utc),
                usegmt=True,
            )

        # Make the request
        with requests.get(self.url, headers=headers, stream=True) as r:
            # Raise InputNotModified if file has not changed
            if r.status_code == 304:
                assert conditional
                raise InputNotModified

            # Check for other HTTP errors
            r.raise_for_status()

            # Update fetch_time and last_modified
            self.fetch_time = datetime.now(timezone.utc)
            if last_modified_str := r.headers.get("Last-Modified"):
                self.last_modified = parsedate_to_datetime(last_modified_str)
                assert self.last_modified.tzinfo is timezone.utc
            else:
                self.last_modified = DATETIME_MIN_UTC
                logging.getLogger("resource").error(
                    "%s did not send the Last-Modified header",
                    urlparse(r.url).netloc,
                )

            # Yield the content
            for chunk in r.iter_content(chunk_size=None, decode_unicode=False):
                yield chunk


class FixStopSequence(Task):
    def __init__(self, gtfs_resource: str) -> None:
        super().__init__()
        self.gtfs_resource = gtfs_resource

    def execute(self, r: TaskRuntime) -> None:
        zip_path = r.resources[self.gtfs_resource].stored_at
        with TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            self.extract_gtfs(temp_dir, zip_path)
            self.fixup_file(temp_dir / "stop_times.txt")
            self.compress_gtfs(zip_path, temp_dir)

    def extract_gtfs(self, dst: Path, src: Path) -> None:
        self.logger.info("Extracting %s", src.name)
        with ZipFile(src, "r") as arch:
            to_extract = filter(
                lambda f: "/" not in f.filename and f.filename.endswith(".txt"),
                arch.infolist(),
            )
            arch.extractall(dst, to_extract)

    def compress_gtfs(self, dst: Path, src: Path) -> None:
        self.logger.info("Updating %s", dst.name)
        with ZipFile(dst, "w", ZIP_DEFLATED) as arch:
            for f in src.iterdir():
                if f.is_file() and f.suffix == ".txt":
                    arch.write(f, f.name)

    def fixup_file(self, file: Path) -> None:
        self.logger.info("Fixing %s", file.name)
        old_content = file.read_text(encoding="utf-8-sig", newline="")
        new_content = self.fixup_file_content(old_content)
        file.write_text(new_content, encoding="utf-8", newline="")

    def fixup_file_content(self, old: str) -> str:
        # Read the old file
        reader = csv.DictReader(io.StringIO(old, newline=""))
        if reader.fieldnames is None or "stop_sequence" in reader.fieldnames:
            self.logger.warning("stop_sequence already present, skipping")
            return old

        # Order all rows by (trip_id, departure_time).
        # Since Python's `sort` is stable, if there are two stop-times with the same
        # time, we're keeping the original ordering.
        rows = list(reader)
        rows.sort(key=itemgetter("trip_id", "departure_time"))

        # Assign stop_sequence
        for _, trip_rows in groupby(rows, key=itemgetter("trip_id")):
            for idx, row in enumerate(trip_rows):
                row["stop_sequence"] = str(idx)

        # Write the new file
        new_buf = io.StringIO(newline="")
        writer = csv.DictWriter(new_buf, fieldnames=[*reader.fieldnames, "stop_sequence"])
        writer.writeheader()
        writer.writerows(rows)
        return new_buf.getvalue()


class MergeDuplicateRoutes(Task):
    def execute(self, r: TaskRuntime) -> None:
        routes_by_short_name = defaultdict[str, list[str]](list)
        for route_id, short_name in cast(
            Iterable[tuple[str, str]],
            r.db.raw_execute("SELECT route_id, short_name FROM routes"),
        ):
            routes_by_short_name[short_name].append(route_id)

        with r.db.transaction():
            for route_ids in routes_by_short_name.values():
                route_ids.sort()
                primary = route_ids[0]
                secondaries = route_ids[1:]

                r.db.raw_execute_many(
                    "UPDATE trips SET route_id = ? WHERE route_id = ?",
                    ((primary, secondary) for secondary in secondaries),
                )
                r.db.raw_execute_many(
                    "DELETE FROM routes WHERE route_id = ?",
                    ((secondary,) for secondary in secondaries),
                )


class GorzowGTFS(App):
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "-o",
            "--output",
            default="gorzow_wlkp.zip",
            help="path to output GTFS file",
        )

    def prepare(self, args: argparse.Namespace, options: PipelineOptions) -> Pipeline:
        return Pipeline(
            options=options,
            resources={
                "calendar_exceptions.csv": polish_calendar_exceptions.RESOURCE,
                "gorzow_wlkp.zip": GorzowGTFSResource(),
            },
            tasks=[
                FixStopSequence("gorzow_wlkp.zip"),
                LoadGTFS("gorzow_wlkp.zip"),
                SimplifyCalendars(generate_new_ids=False),
                ExtendCalendarsFromPolishExceptions(
                    resource_name="calendar_exceptions.csv",
                    region=polish_calendar_exceptions.PolishRegion.LUBUSKIE,
                    duration_days=90,
                ),
                ExecuteSQL(
                    statement="UPDATE stops SET name = re_sub('\"{2,}', '\"', name)",
                    task_name="FixDoubleQuotesInStopName",
                ),
                ExecuteSQL(
                    statement=(
                        "UPDATE stops SET name = 'Fieldorfa \"Nila\"' "
                        "WHERE name = 'Fieldorfa\"Nila\"'"
                    ),
                    task_name="FixFieldorfaNilaStopName",
                ),
                MergeDuplicateRoutes(),
                ExecuteSQL(
                    statement=(
                        "UPDATE routes SET color = CASE "
                        "  WHEN type = 0 THEN '339966'"
                        "  WHEN short_name LIKE '5__' THEN '000000'"
                        "  ELSE 'B8D434'"
                        "END"
                    ),
                    task_name="UpdateRouteColor",
                ),
                ExecuteSQL(
                    statement=(
                        "UPDATE routes SET text_color = iif(color = 'B8D434', '000000', 'FFFFFF')"
                    ),
                    task_name="UpdateRouteTextColor",
                ),
                ExecuteSQL(
                    statement="UPDATE routes SET long_name = ''",
                    task_name="ClearRouteLongName",
                ),
                ExecuteSQL(
                    statement=(
                        "UPDATE feed_info SET publisher_name = 'Mikołaj Kuranowski', "
                        "publisher_url = 'https://mkuran.pl/gtfs/'"
                    ),
                    task_name="UpdateFeedInfo",
                ),
                SaveGTFS(headers=GTFS_HEADERS, target=args.output),
            ],
        )


if __name__ == "__main__":
    GorzowGTFS().run()
