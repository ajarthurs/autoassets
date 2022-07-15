# autoassets
A Multi-Asset Autotrader

A streaming Python program that manages multiple and independent assets. An asset is a combination of one or more vehicles (e.g., stock) and a trade policy such as buy-and-hold, price targeting, and various timing strategies over various timeframes. The structure of autoassets divides each asset into two parts: position and strategy, allowing one to mix and match vehicles and trade policies with ease. It is possible to setup multiple assets that share the same vehicle but run different trade policies, and autoassets will process each asset independent of one another. For example, one may trade a stock over the daily and minute timeframes, maintaining a unique cost and position per timeframe (i.e., two assets, each with their own dollar cost average on the same stock). Autoassets is open-ended in terms of structure and maintenance of the trade vehicle and its policy as well as source of data from certain backends. Data such as quotes, historical time-series (and even "volume"-series), and option chains are provided to all assets in a convenient format: the pandas Dataframe [^1]. One can easily query autoassets's internal database, such as scan the option chains for puts (or calls) that pays at least a user-specified premium. There are also data analytics such as producing risk/reward frontiers over multiple trade vehicles, finding pivot points in option chains and other functional logic.

# Requirements:
* Python 3.10 or greater
* Pandas 1.4 or greater
* TDA Backend Module: [`python-tda`](https://github.com/ajarthurs/python-tda)

# Installation:
TODO: Create `setup.py`

# Getting Started:
```bash
cp autoassets/templates/settings.py ${PWD}
cp autoassets/templates/assets.json ${PWD}

# Edit `settings.py` to setup the backend(s) such as setting API keys required for access, and write asset
# classes to define combinations of trade vehicles, trade policies and their respective sources of data.

# Edit `assets.json` to setup the assets themselves. Each asset must define the following fields:
# `class`, `budget`, `enable`. The field `class` must match one of the asset classes defined in `settings.py`.

python autoassets/bin/asset_trader.py
```
At this point, autoassets will stream in requested data and manage each asset accordingly. To stop the program, send a `SIGHUP` signal or press `Ctrl-C`.

# Status:
* This project is currently in alpha, meaning that major dependency-breaking changes will happen without notice. The user should keep a copy that works for their setup before upgrading. That being said, this project is a working prototype that will fulfill common use cases such as accumulating stock and selling premium in options.
* Only TD Ameritrade (TDA) is supported for the backend, and more specifically this project requires [`python-tda`](https://github.com/ajarthurs/python-tda). Do not substitute another TDA API wrapper. This may likely change in the future should TDA release their own API wrapper.
* I am currently restructuring autoassets to allow the user to develop their own vehicles and trade policies outside this project. There are several other features in consideration such as migrating from the JSON-formatted flat file to a database.
* There is no frontend. The user must edit Python and JSON code by hand or otherwise develop their own frontend.

[^1]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
