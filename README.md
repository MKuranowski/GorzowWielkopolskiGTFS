GorzowWielkopolskiGTFS
======================

Creates a cleaned-up GTFS file for [MZK Gorzów Wielkopolski](https://mzk-gorzow.com.pl/)
from files published at <https://bip.mzk-gorzow.com.pl/gtfs.html>.


Running
-------

GZM GTFS is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ python gorzow_wlkp_gtfs.py
```

The resulting schedules will be put in a file called `gorzow_wlkp.zip`.

See `python gorzow_wlkp_gtfs.py --help` for a list of all available options.


License
-------

_GorzowWielkopolskiGTFS_ is provided under the MIT license, included in the `LICENSE` file.
