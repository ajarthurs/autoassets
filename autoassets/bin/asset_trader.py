# Created on Fri Jun 05 2020
# 
# This file is a part of autoassets.
# autoassets is a free and open-source asset manager written by
# Aaron Arthurs ("the Author") to facilitate development and execution
# across a variety of trade strategies at the user's discretion.
# 
# Copyright (c) 2020-2022, Aaron Arthurs <aajarthurs@gmail.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""
Asset Trader.
A program that trades a user-defined list of assets given their respective budgets and strategies.
"""

import autoassets
import autoassets.positioning
import autoassets.strategy
import logging
import logging.config
import pickle
import settings
import signal
import sys
import yaml

global_flags = {'done': False}
logger = logging.getLogger(__file__)

def main():
    global global_flags
    signal.signal(signal.SIGINT, sh_mark_done)
    assets = autoassets.load_assets(settings.assets_definition, settings.assets_path)
    quote_db = autoassets.fetch_quotes(assets)
    instrument_db = autoassets.fetch_historical_data(assets)
    option_chain_db = autoassets.fetch_option_chains(assets, quote_db)
    streams = autoassets.connect_to_streams(settings.backends_setting, assets)
    autoassets.subscribe_to_historical_data(streams, assets, instrument_db, settings.assets_path)
    autoassets.subscribe_to_option_data(streams, assets, option_chain_db, quote_db)
    autoassets.subscribe_to_quotes(streams, assets, instrument_db, option_chain_db, quote_db, settings.backends_setting)
    autoassets.listen_to_streams(streams, global_flags)
    autoassets.disconnect_from_streams(streams)
    autoassets.store_assets(assets, settings.assets_path)
    print('DONE')
#END: main

def sh_mark_done(sig, frame):
    global global_flags
    global_flags['done'] = True
#END: sh_mark_done

if(__name__ == '__main__'):
    with open('logging.yaml') as f:
        y = yaml.load(f, yaml.FullLoader)
        logging.config.dictConfig(y)
    main()
