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
Trend strategy.

Sets buy and sell price targets over linear regression. Price targets depend on a reference price (`anchor_price`) and the instrument's volatility over a given frame (time- or volume-based).

`alpha` is a signed coefficient applied to trades available. For example, an `alpha` of 1.0 (default) applies nominal resistance to exhaustion, meaning that the strategy will target prices further away from the reference price as fewer trades become available. A higher `alpha` increases the 'resistance to exhaustion' effect, which can lead towards half-invested assets. An `alpha` of zero (0) disables the 'resistance to exhaustion' feature. Finally, a negative `alpha` accelerates exhaustion.
"""

import autoassets.positioning
import autoassets.strategy
import logging
import math
import numpy as np

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
    return [strategy['instrument']['backend']]
#END: backends

def execute(asset, instrument_db, option_chain_db, quote_db, backend_setting):
    """
    Buy oversold and sell overbought according to present trend.

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
    strategy = asset['definition']['strategy']
    strategy_instrument = strategy['instrument']
    frame = strategy_instrument['frame']
    ticker = strategy_instrument['ticker']
    history = instrument_db[frame][ticker]
    if history.empty:
        return
    window = -1
    if 'window' in strategy_instrument:
        window = min(len(history.index), strategy_instrument['window'])
    x = history.index[0:window].view('int64')
    linear_coeff, linear_residuals_squared, _, _, _ = np.polyfit(
        x,
        history[0:window][autoassets.ChartBarField.CLOSE_PRICE], #y
        deg=1,
        full=True,
    )
    slope = linear_coeff[0]
    linear_func = np.poly1d(linear_coeff)
    linear_residual = math.sqrt(linear_residuals_squared[0] / len(x))
    channel_width = 2.0 * linear_residual
    window_xdelta = x[0] - x[-1]
    window_ydelta = window_xdelta * slope
    trend_up   = (slope > 0.0)
    trend_flat = (abs(window_ydelta) < channel_width)
    trend_down = (slope < 0.0)
    anchor_price = linear_func(x[0])
    availability = autoassets.positioning.availability(asset, quote_db, option_chain_db)
    position_delta = autoassets.positioning.delta(asset, option_chain_db)
    current_price = history.iloc[0][autoassets.ChartBarField.CLOSE_PRICE]
    previous_price = history.iloc[1][autoassets.ChartBarField.CLOSE_PRICE]
    min_price_offset = max(linear_residual, 0.0001 * current_price)
    alpha = 1.0 if 'alpha' not in strategy else strategy['alpha']
    sell_pt = anchor_price + min_price_offset * (1.0 + alpha * availability['bullish_vacancy'])
    buy_pt = anchor_price - min_price_offset * (1.0 + alpha * availability['bearish_vacancy'])
    logger.debug('{}: mark = {}; sell_pt = {}, buy_pt = {}; anchor_price = {}; window_ydelta = {}; channel_width = {}; min_price_offset = {}; slope = {}'.format(ticker, current_price, sell_pt, buy_pt, anchor_price, window_ydelta, channel_width, min_price_offset, slope))
    if previous_price > sell_pt and current_price <= sell_pt: # SELL ZONE
        logger.debug('SELL {}'.format(ticker))
        autoassets.positioning.place_bearish_trade(asset, quote_db, option_chain_db, backend_setting)
    elif previous_price < buy_pt and current_price >= buy_pt: # BUY ZONE
        logger.debug('BUY {}'.format(ticker))
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
    return [strategy['instrument']]
#END: instruments
