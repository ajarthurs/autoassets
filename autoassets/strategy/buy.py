# Created on Fri Jun 05 2020
# 
# This file is a part of autoassets.
# autoassets is a free and open-source asset manager written by
# Aaron Arthurs ("the Author") to facilitate development and execution
# across a variety of trade strategies at the user's discretion.
# 
# Copyright (c) 2020-present, Aaron Arthurs <aajarthurs@gmail.com>
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
The "just buy" strategy.

"""

import autoassets.positioning
import autoassets.strategy
import logging

logger = logging.getLogger(__name__)

def backends(strategy):
    """
    Return referenced backends.

    Parameters
    ----------
    strategy: dict
        Strategy specifications.

    Returns
    -------
    [dict]:
        Unique list of backends.
    """
    return []
#END: backends

def execute(asset, instrument_db, option_chain_db, quote_db, backend_setting):
    """
    Buy.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    instrument_db: dict
        Instrument database.

    option_chain_db: dict
        Option chain database.

    quote_db: dict
        Quote database.

    backend_setting: dict
        Backend setting.
    """
    autoassets.positioning.place_bullish_trade(asset, quote_db, option_chain_db, backend_setting)
#END: execute

def instruments(strategy):
    """
    Return referenced instruments.

    Parameters
    ----------
    strategy: dict
        Strategy specifications.

    Returns
    -------
    [dict]:
        List of instruments.
    """
    return []
#END: instruments
