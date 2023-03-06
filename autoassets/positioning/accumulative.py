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
Accumulative positioning.

A combination of income positioning and asset accumulation.
"""

import autoassets
import logging
import math
import pandas as pd

logger = logging.getLogger(__name__)

def availability(asset, quote_db, option_chain_db):
    """
    Calculate available units of asset to buy and sell.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    dict:
        Availability specifications.
    """
    positioning = asset['definition']['positioning']
    budget_specs = autoassets.budget(asset)
    budget_short, budget_long = budget_specs['amount']
    if budget_short != 0.0:
        logger.error('Short budget must be 0')
        return None
    if budget_specs['unit'] != autoassets.BudgetUnit.DOLLAR:
        logger.error('Invalid budget unit: {}; expected DOLLAR unit'.format(budget_specs['unit']))
        return None
    ticker = positioning['ticker']
    quote = quote_db[ticker]
    ask_price = quote[autoassets.QuoteField.ASK_PRICE]
    position = 0 if 'position' not in asset else asset['position']
    if 'ath' not in asset or ask_price > asset['ath']:
        asset['ath'] = ask_price
    if 'profit' in asset: # Reinvest profit.
        budget_long += asset['profit']
    budget_long_shares = budget_long / ask_price
    denomination_long = 1
    #denomination_long = max(1, int(positioning['denomination'] * budget_long))
    bullish_trades_float = (budget_long_shares - position) / denomination_long
    bearish_trades_float = position / denomination_long
    total_trades_float = bullish_trades_float + bearish_trades_float
    return {
        'budget_long': budget_long,
        'budget_short': 0.0,
        'bullish_trades': int(bullish_trades_float),
        'bearish_trades': int(bearish_trades_float),
        'bullish_vacancy': (bullish_trades_float / total_trades_float),
        'bearish_vacancy': (bearish_trades_float / total_trades_float),
        'denomination_long' : 1,
        'denomination_short' : 0,
    }
#END: availability

def cost_per_unit(asset, option_chain_db):
    """
    Calculate cost per share for the given asset. Note that cost/unit is always positive.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    float:
        Cost per unit.
    """
    if 'position' in asset and asset['position'] != 0:
        return asset['cost'] / asset['position']
    else:
        return 0.0
#END: cost_per_unit

def cost_with_margin(asset):
    """
    Calculate total cost of given asset. Note that cost is always positive because it accounts for margin requirements.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    float:
        Asset's total cost.
    """
    return (0.0 if 'cost' not in asset else asset['cost'])
#END: cost_with_margin

def delta(asset, option_chain_db):
    """
    Return asset's net delta.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    int:
        Asset's net delta.
    """
    return (0 if 'position' not in asset else asset['position'])
#END: delta

def instruments(positioning, require_optionable=False):
    """
    Return referenced instruments.

    Parameters
    ----------
    positioning: dict
        Positioning specifications.

    require_optionable: bool (default: False)
        Return optionable instruments only.

    Returns
    -------
    [dict]:
        List of instruments.
    """
    return [{
        'backend': positioning['backend'],
        'ticker': positioning['ticker'],
    }]
#END: instruments

def market_value(asset, quote_db, option_chain_db):
    """
    Calculate total market value of asset. Note that market value can be negative, signifying short positions.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    (float, float):
        Market value and total running profit of asset.
    """
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    realized_profit = 0.0 if 'profit' not in asset else asset['profit']
    mv = delta(asset, option_chain_db) * quote_db[ticker][autoassets.QuoteField.MARK_PRICE]
    return (mv, mv - cost_with_margin(asset))
#END: market_value

def neutralize(asset, quote_db, option_chain_db, backend_setting):
    """
    Neutralize (flatten) position.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    backend_setting: dict
        Backend setting.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    position = 0 if 'position' not in asset else asset['position']
    if not _maybe_place_stock_order(asset, quote_db, option_chain_db, backend_setting, -position, follow_last_trade=False):
        return False
    logger.info('Neutralized {}.'.format(asset))
    return True
#END: neutralize

def place_bearish_trade(asset, quote_db, option_chain_db, backend_setting):
    """
    Place a bearish trade.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    backend_setting: dict
        Backend setting.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    availability_specs = availability(asset, quote_db, option_chain_db)
    if availability_specs['bearish_trades'] == 0:
        logger.debug('{}: Abort trade: No bearish trades available.'.format(ticker))
        return False
    position = 0 if 'position' not in asset else asset['position']
    unit_denomination = min(position, availability_specs['denomination_long'])
    if not _maybe_place_stock_order(asset, quote_db, option_chain_db, backend_setting, -unit_denomination):
        return False
    logger.info('Placed bearish trade on {}.'.format(asset))
    return True
#END: place_bearish_trade

def place_bullish_trade(asset, quote_db, option_chain_db, backend_setting):
    """
    Place a bullish trade.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    backend_setting: dict
        Backend setting.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    availability_specs = availability(asset, quote_db, option_chain_db)
    if availability_specs['bullish_trades'] == 0:
        logger.debug('{}: cannot buy: no bullish trades available.'.format(ticker))
        return False
    position = 0 if 'position' not in asset else asset['position']
    unit_denomination = availability_specs['denomination_long']
    if not _maybe_place_stock_order(asset, quote_db, option_chain_db, backend_setting, unit_denomination):
        return False
    logger.info('Placed bullish trade on {}.'.format(asset))
    return True
#END: place_bullish_trade

def primary_quote(asset, quote_db, option_chain_db):
    """
    Return representative quote from position.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    dict:
        Primary quote.
    """
    ticker = positioning['ticker']
    return quote_db[ticker]
#END: primary_quote

def probe(asset, instrument_db, option_chain_db, quote_db, backend_setting):
    """
    Scale into drawdowns.

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
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    logger.debug('{}: CDCA = {} (DCA = {}).'.format(ticker, _cdca(asset, option_chain_db), cost_per_unit(asset, option_chain_db)))
    ask_price = quote_db[ticker][autoassets.QuoteField.ASK_PRICE]
    availability_specs = availability(asset, quote_db, option_chain_db)
    occupance = availability_specs['bearish_vacancy']
    drawdown = 1.0 - (ask_price / asset['ath'])
    if drawdown > occupance: # Increase occupance to match market's current drawdown.
        adjustment = drawdown - occupance
        quantity = int(adjustment * availability_specs['budget_long'] / ask_price)
    else:
        quantity = 0
    if quantity > 0:
        _maybe_place_stock_order(asset, quote_db, option_chain_db, backend_setting, quantity, follow_last_trade=False)
        logger.info('{}: Bought to adjust to drawdown; {} shares; adjustment = {}; total budget = {}.'.format(ticker, quantity, adjustment, availability_specs['budget_long']))
#END: probe

#################
# LOW-LEVEL API #
#################

def _cdca(asset, option_chain_db):
    """
    Calculate the asset's compounded cost per unit.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain_db: dict
        Option chain database.

    Returns
    -------
    float:
        Asset's compounded cost per unit.
    """
    unit_cost = cost_per_unit(asset, option_chain_db)
    now = pd.to_datetime('now', utc=True)
    acquired_at = now if 'acquired_at' not in asset else pd.to_datetime(asset['acquired_at'])
    return unit_cost * math.pow(1.0 + asset['expected_rate'], (now - acquired_at).total_seconds() / (3600.0 * 24.0 * 365.25))
#END: _cdca

def _maybe_place_stock_order(asset, quote_db, option_chain_db, backend_setting, quantity, follow_last_trade=True):
    """
    Place a single-legged order.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    quote_db: dict
        Quote database.

    option_chain_db: dict
        Option chain database.

    backend_setting: dict
        Backend setting.

    quantity: int
        Number of shares to buy if positive, or sell if negative.

    follow_last_trade: bool (default: True)
        Whether or not to follow last trade.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    positioning = asset['definition']['positioning']
    enable_trades = False if 'enable_trades' not in positioning else positioning['enable_trades']
    account_id = backend_setting['account_id']
    backend = positioning['backend']
    instrument_type = positioning['instrument_type']
    ticker = positioning['ticker']
    availability_specs = availability(asset, quote_db, option_chain_db)
    if quantity < 0: # SELL
        direction = autoassets.OrderDirection.SELL
        price = quote_db[ticker][autoassets.QuoteField.BID_PRICE]
        vacancy = availability_specs['bearish_vacancy']
    elif quantity > 0: # BUY
        direction = autoassets.OrderDirection.BUY
        price = quote_db[ticker][autoassets.QuoteField.ASK_PRICE]
        vacancy = availability_specs['bullish_vacancy']
    else: # Do nothing.
        return False
    position = 0 if 'position' not in asset else asset['position']
    unit_cost = cost_per_unit(asset, option_chain_db)
    compounded_unit_cost = _cdca(asset, option_chain_db)
    follow_last_trade_alpha = 0.001 if 'follow_last_trade_alpha' not in positioning else positioning['follow_last_trade_alpha']
    min_price_offset = follow_last_trade_alpha * price * vacancy
    sign_quantity = math.copysign(1, quantity)
    # Compare last trade versus candidate trade.
    if follow_last_trade and position != 0 and 'last_trade_price' in asset: # Follow last trade.
        target_price = asset['last_trade_price'] - (sign_quantity * min_price_offset)
        if (sign_quantity * price) > (sign_quantity * target_price): # Price is worse than last trade.
            logger.debug('{}: Abort trade: Price {} is worse than target {} (last_trade {}; min_price_offset {}; vacancy {}).'.format(ticker, price, target_price, asset['last_trade_price'], min_price_offset, vacancy))
            return False
    # Compare cost average versus candidate closing trade.
    if quantity < 0: # Closing/scaling back long position.
        # Do not close position for worse than cost.
        min_sell_price = asset['ath'] * vacancy + min_price_offset
        if price < compounded_unit_cost or price < min_sell_price:
            logger.debug('{}: Abort trade: Price {} is worse than compounded unit cost {} ({} nominal); min_sell_price = {}.'.format(ticker, price, compounded_unit_cost, unit_cost, min_sell_price))
            return False
    # Place order and update position.
    if enable_trades:
        if not backend.place_market_order(account_id, instrument_type, ticker, direction, abs(quantity)):
            return False
    now = pd.to_datetime('now', utc=True)
    acquired_at = now if 'acquired_at' not in asset else pd.to_datetime(asset['acquired_at'])
    cost = 0.0 if 'cost' not in asset else asset['cost']
    cost_prime = cost + price * quantity
    if quantity < 0: # Closed/scaled back long position.
        asset['cost'] = cost + unit_cost * quantity
        if 'profit' not in asset:
            asset['profit'] = 0.0
        asset['profit'] += (asset['cost'] - cost_prime)
    else: # Opened position.
        asset['cost'] = cost_prime
        # Average in time of acquisition.
        asset['acquired_at'] = (((now - acquired_at) * (quantity / (quantity + position))) + acquired_at).isoformat()
    asset['position'] = position + quantity
    if asset['position'] == 0:
        del asset['position']
        del asset['cost']
        del asset['acquired_at']
    asset['last_trade_price'] = price
    return True
#END: _maybe_place_stock_order
