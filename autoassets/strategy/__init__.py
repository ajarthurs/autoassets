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
Strategy package.
"""

import autoassets
import autoassets.positioning
import autoassets.schedule
import logging
import math
import pandas as pd
from enum import Enum, auto

logger = logging.getLogger(__name__)

class ChartBarField(Enum):
    """
    Instrument bar field.
    """
    SLOPE = auto()
    TR = auto()

def active_instruments(assets):
    """
    Return a unique list of instruments referenced by strategy.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    Returns
    -------
    [dict]:
        Unique list of active instruments.
    """
    active_instruments = []
    for asset in assets:
        if not autoassets.asset_active(asset):
            continue
        strategy = asset['definition']['strategy']
        method = strategy['method']
        instruments = method.instruments(strategy)
        active_instruments += instruments
    return list(map(dict, frozenset([frozenset(i.items()) for i in active_instruments])))
#END: active_instruments

def backends(strategy):
    """
    Return backends.

    Parameters
    ----------
    strategy: dict
        Strategy specifications.

    Returns
    -------
    [module]:
        List of backend modules (see autoassets.backend).
    """
    return strategy['method'].backends(strategy)
#END: backends

def cb_run_strategies(instruments, data):
    """
    Execute each listed asset's strategy.

    Parameters
    ----------
        instruments: [dict]
            List of instruments on which to run strategies.

        data: dict('assets', 'instrument_db', 'option_chain_db', 'quote_db', 'backend_setting')
            Assets and their quotes and history.
    """
    assets = data['assets']
    instrument_db = data['instrument_db']
    option_chain_db = data['option_chain_db']
    quote_db = data['quote_db']
    backend_setting = data['backend_setting']
    for asset in assets:
        #XXX: Filter which assets to run given `instruments`.
        ##instruments = autoassets.positioning.active_instruments([asset])
        #asset_tickers = autoassets.get_tickers_from_instruments(instruments)
        #if not any(ticker in asset_tickers for ticker in tickers):
        #    continue
        if not autoassets.asset_active(asset):
            continue
        name = '(No Name)' if 'name' not in asset else asset['name']
        positioning = asset['definition']['positioning']
        structure = positioning['structure']
        cost = structure.cost_with_margin(asset)
        market_value, profit = structure.market_value(asset, quote_db, option_chain_db)
        logger.debug('"{}": Delta = {}; Cost = {}; MV = {}; Profit = {}.'.format(
            name,
            structure.delta(asset, option_chain_db),
            cost,
            market_value,
            profit,
            ))
        if positioning['ticker'] in option_chain_db and len(option_chain_db[positioning['ticker']]) > 0:
            with open('{}_OC.txt'.format(positioning['ticker']), 'w') as f:
                f.write(option_chain_db[positioning['ticker']].to_string(
                    index=False,
                    columns=[
                        autoassets.OptionContractField.DESCRIPTION,
                        autoassets.OptionContractField.BID_PRICE,
                        autoassets.OptionContractField.ASK_PRICE,
                        autoassets.OptionContractField.EXTRINSIC_MARK_PRICE,
                        #autoassets.OptionContractField.LAST_PRICE,
                        autoassets.OptionContractField.DELTA,
                        #autoassets.OptionContractField.GAMMA,
                        #autoassets.OptionContractField.THETA,
                        #autoassets.OptionContractField.VOLATILITY,
                        #autoassets.OptionContractField.OPEN_INTEREST,
                        autoassets.OptionContractField.VOLUME,
                        autoassets.OptionContractField.VOLUME_BIAS,
                    ],
                    ))
        strategy = asset['definition']['strategy']
        if not autoassets.schedule.is_now_normal_market_hours():
            continue
        start_at = None if 'start_at' not in strategy else strategy['start_at']
        neutralize_at = None if 'neutralize_at' not in strategy else strategy['neutralize_at']
        neutralize_on_close = False if 'neutralize_on_close' not in strategy else strategy['neutralize_on_close']
        if not autoassets.schedule.is_now_tradable(start_at, neutralize_at):
            now_utc = pd.to_datetime('now', utc=True)
            now     = now_utc.tz_convert('America/New_York')
            now_time = now.time()
            if (start_at is None and now_time < autoassets.schedule.default_start_time) or (start_at is not None and now_time < start_at):
                continue
            if neutralize_at is not None or neutralize_on_close:
                autoassets.positioning.neutralize(asset, quote_db, option_chain_db, backend_setting)
            continue
        #block_new_trades_at = None if 'block_new_trades_at' not in strategy else strategy['block_new_trades_at']
        #if not autoassets.schedule.is_now_tradable(start_at, block_new_trades_at):
        #    continue
        strategy['method'].execute(asset, instrument_db, option_chain_db, quote_db, backend_setting)
#END: cb_run_strategies

def slope(history, xdelta, bar=1):
    """
    Return slope over an x-delta at bar.

    Parameters
    ----------
    history: pd.DataFrame
        Bar history.

    xdelta: int
        Length over which to measure slope.

    bar: int (default: 1, most recent historical bar)
        Bar index. Note that 0 references the active bar.

    Returns
    -------
        float:
            Slope.
    """
    if (bar + xdelta) >= len(history):
        return math.nan
    x = history.index[bar:bar+xdelta].astype(np.int64)
    y = history[bar:bar+xdelta][[autoassets.ChartBarField.HIGH_PRICE, autoassets.ChartBarField.LOW_PRICE, autoassets.ChartBarField.CLOSE_PRICE]].mean(axis=1)
    linear_coeff, linear_residuals_squared, _, _, _ = np.polyfit(
        x,
        y,
        deg=1,
        full=True,
    )
    return linear_coeff[0]
#END: slope

def true_range(history, bar=1):
    """
    Return true range at bar.

    Parameters
    ----------
    history: pd.DataFrame
        Bar history.

    bar: int (default: 1, most recent historical bar)
        Bar index. Note that 0 references the active bar.

    Returns
    -------
        float:
            True range.
    """
    if bar >= (len(history) - 1):
        return math.nan
    prior_close = history[autoassets.ChartBarField.CLOSE_PRICE].iloc[bar+1]
    high = history[autoassets.ChartBarField.HIGH_PRICE].iloc[bar]
    low = history[autoassets.ChartBarField.LOW_PRICE].iloc[bar]
    return max(
        high - low,
        high - prior_close,
        prior_close - low,
    )
#END: true_range
