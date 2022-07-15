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
Short put backratio (a.k.a. broken-wing butterfly) positioning.
"""

import autoassets
import autoassets.positioning
import copy
import logging
import math
import pandas as pd
import numpy as np
import time
from enum import Enum, auto
from functools import reduce

logger = logging.getLogger(__name__)

UNITS_PER_CONTRACT = 100 #XXX: Covers majority of options contracts, but consider adding a contract field and use 100 units/contract as default.
COMMISSION_PER_CONTRACT = 1.15 #dollars
NONIDEAL_ADJUSTMENT_FRACTION = 0.0 # fraction of premium
MIN_LONG_DELTA = 0.03
#MAX_LONG_DELTA = 1.0 - MIN_LONG_DELTA
MAX_LONG_DELTA = 0.50
MAX_BUY_MARGIN_PREMIUM = 0.15
MIN_COST_PREMIUM = 0.05 + MAX_BUY_MARGIN_PREMIUM
MIN_SELL_PREMIUM = 3.0 * MIN_COST_PREMIUM # 1x to cover cost, 2x to cover inefficiency and 3x for profit.
LONG_PROFIT_TARGET = 0.5
SHORT_PROFIT_TARGET = 0.50
SHORT_LOSS_LIMIT = 2.0 - SHORT_PROFIT_TARGET

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
    ticker = positioning['ticker']
    quote = quote_db[ticker]
    option_chain = option_chain_db[ticker]
    if budget_specs['unit'] == autoassets.BudgetUnit.SHARE:
        logger.error('Invalid budget unit: {}; expected DOLLAR unit'.format(budget_specs['unit']))
        return None
    cost = _total_cost(asset)
    denomination = positioning['denomination']
    available_trades_float = (budget_long - cost) / denomination
    vacancy = (budget_long - cost) / budget_long
    return {
        'bullish_trades': int(available_trades_float),
        'bearish_trades': int(available_trades_float),
        'bullish_vacancy': vacancy,
        'bearish_vacancy': vacancy,
        'denomination_long' : denomination,
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
    if 'leg_db' not in asset:
        return 0.0
    position_delta = delta(asset, option_chain_db)
    if position_delta == 0:
        return 0.0
    return _total_cost(asset) / position_delta
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
    if 'leg_db' not in asset:
        return 0.0
    return _total_cost(asset)
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
    if asset['ticker'] not in option_chain_db or 'leg_db' not in asset:
        return 0
    option_chain = option_chain_db[asset['ticker']]
    return reduce(
            lambda x, symbol: x + int(option_chain.loc[symbol][autoassets.OptionContractField.DELTA] * asset['leg_db'][symbol]['quantity'] * asset['leg_db'][symbol]['shares_per_contract']),
            asset['leg_db'], 0)
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
    i = [{
        'backend': positioning['backend'],
        'ticker': positioning['ticker'],
        'max_dte': positioning['max_dte'],
        'strike_count': positioning['strike_count'],
    }]
    #if not require_optionable:
    #    i.append({
    #            'backend': positioning['backend'],
    #            'ticker': positioning['iv_ticker'],
    #            })
    return i
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
    option_chain = option_chain_db[ticker]
    return _total_profit(asset, option_chain)
#END: market_value

def neutralize(asset, quote_db, option_chain_db, backend_setting):
    """
    Neutralize (flatten) position one contract at a time.

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
        True if fully neutralized; False otherwise.
    """
    if 'leg_db' not in asset:
        return True
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    option_chain = option_chain_db[ticker]
    now = pd.to_datetime('now', utc=True)
    today = now.date()
    option_chain = option_chain_db[ticker]
    def __closing_cb(leg_df, side_df, contract_df, position_df):
        symbol = contract_df[autoassets.OptionContractField.SYMBOL]
        #opex_date = contract_df[autoassets.OptionContractField.OPEX].date()
        opex = contract_df[autoassets.OptionContractField.OPEX]
        #if opex_date == today:
        if now >= opex:
            logger.debug('now = {}; opex = {}.'.format(now, opex))
            return _place_single_order(asset, backend_setting,
                    quantity=-position_df['quantity'],
                    contract_df=contract_df,
                    )
        return False
    #END: __closing_cb
    _scan_and_adjust(asset, option_chain, backend_setting, __closing_cb)
    if 'leg_db' not in asset:
        return True
    return False
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
    return False
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
    if ticker not in quote_db or ticker not in option_chain_db:
        logger.debug('Missing quote and/or option chain for {}.'.format(ticker))
        return False
    availability_specs = availability(asset, quote_db, option_chain_db)
    vacancy = availability_specs['bullish_vacancy']
    denomination = availability_specs['denomination_long']
    logger.debug('Bullish vacancy = {}.'.format(vacancy))
    option_chain = option_chain_db[ticker]
    mark = quote_db[ticker][autoassets.QuoteField.MARK_PRICE]
    #opex = option_chain[autoassets.OptionContractField.OPEX].iloc[0]
    # Check budget.
    if availability_specs['bullish_trades'] == 0:
        logger.debug('Abort trade: Zero bullish trades available.')
        return False
    # Check for beyond-DTE0 position; otherwise, determine nearest OPEX in which to open new position.
    leg_df, call_df, put_df = _leg_dataframe(asset)
    now = pd.to_datetime('now', utc=True)
    tomorrow = now + pd.to_timedelta(1, 'D')
    duration_put_df = [] if len(put_df) == 0 else put_df[put_df['opex'] >= tomorrow]
    if len(duration_put_df) > 0:
        logger.debug('Abort trade: Already have duration position.')
        return False
    duration_option_chain = option_chain[option_chain[autoassets.OptionContractField.OPEX] >= tomorrow]
    nearest_contract = duration_option_chain.sort_values(by=autoassets.OptionContractField.OPEX, ascending=True).iloc[0]
    opex = nearest_contract[autoassets.OptionContractField.OPEX]
    dte = (opex - now).days
    # Find long put.
    long_put_query = option_chain[
        (option_chain[autoassets.OptionContractField.OPEX] == opex) &
        (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.PUT) &
        (option_chain[autoassets.OptionContractField.DELTA] >= -0.16)
        ]
    if len(long_put_query) == 0:
        logger.debug('Abort trade (buy put): Cannot find put at or below 16 delta.')
        return False
    long_put_symbol = long_put_query[autoassets.OptionContractField.STRIKE].idxmax()
    long_put = option_chain.loc[long_put_symbol]
    long_put_premium = long_put[autoassets.OptionContractField.ASK_PRICE]
    # Find short puts to finance long put.
    target_premium = ((long_put_premium + (dte * asset['target_premium_per_day'])) / 2.0) + 2.0 * MAX_BUY_MARGIN_PREMIUM
    logger.debug('{}: Target premium = {}; DTE = {}, long-put premium = {}.'.format(ticker, target_premium, dte, long_put_premium))
    if target_premium < MIN_SELL_PREMIUM:
        logger.debug('Abort trade (short put): Target premium {} is too low.'.format(target_premium))
        return False
    short_put_query = option_chain[
        (option_chain[autoassets.OptionContractField.OPEX] == opex) &
        (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.PUT) &
        (option_chain[autoassets.OptionContractField.BID_PRICE] >= target_premium)
        ]
    if len(short_put_query) == 0:
        logger.debug('Abort trade (short put): Cannot find premium at or above {}.'.format(target_premium))
        return False
    short_put_symbol = short_put_query[autoassets.OptionContractField.STRIKE].idxmin()
    if short_put_symbol == long_put_symbol:
        logger.debug('Abort trade (short put): Conflict on {}.'.format(short_put_symbol))
        return False
    short_put = option_chain.loc[short_put_symbol]
    short_put_premium = short_put[autoassets.OptionContractField.BID_PRICE]
    # Find long call to limit margin.
    margin_put_query = option_chain[
        (option_chain[autoassets.OptionContractField.OPEX] == opex) &
        (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.PUT) &
        (option_chain[autoassets.OptionContractField.ASK_PRICE] <= MAX_BUY_MARGIN_PREMIUM)
        ]
    if len(margin_put_query) == 0:
        logger.debug('Abort trade (margin put): No {}-dollar puts detected.'.format(MAX_BUY_MARGIN_PREMIUM))
        return False
    margin_put_symbol = margin_put_query[autoassets.OptionContractField.STRIKE].idxmax()
    margin_put = option_chain.loc[margin_put_symbol]
    margin_put_premium = margin_put[autoassets.OptionContractField.ASK_PRICE]
    # Place backratio trade.
    if not _place_spread_order(asset, backend_setting,
            quantity=denomination,
            buy_df=long_put,
            sell_df=short_put,
            ):
        return False
    if not _place_spread_order(asset, backend_setting,
            quantity=denomination,
            buy_df=margin_put,
            sell_df=short_put,
            ):
        return False
    net_credit = 2 * short_put_premium - long_put_premium - margin_put_premium
    logger.debug('{}: Sold put-backratio, {}-2x{} (margin at {}), for {} credit (2x{}-{}-{}).'.format(ticker, long_put_symbol, short_put_symbol, margin_put_symbol, net_credit, short_put_premium, long_put_premium, margin_put_premium))
    asset['last_trade_price'] = mark
    logger.info('Placed bullish trade on\n{}.'.format(asset))
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
    Close current spreads for near maximum profit, if any.

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
    if 'leg_db' not in asset:
        return
    positioning = asset['definition']['positioning']
    ticker = positioning['ticker']
    option_chain = option_chain_db[ticker]
    mark = quote_db[ticker][autoassets.QuoteField.MARK_PRICE]
    opex = option_chain[autoassets.OptionContractField.OPEX].iloc[0]
    def __closing_cb(leg_df, side_df, contract_df, position_df):
        symbol = contract_df[autoassets.OptionContractField.SYMBOL]
        contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
        if position_df['quantity'] > 0: # Skip long contracts.
            return False
        premium = contract_df[autoassets.OptionContractField.ASK_PRICE]
        # Buy back short contract for minimum premium.
        if premium <= MAX_BUY_MARGIN_PREMIUM:
            logger.info('Detected max-profit on contract; value={}:\n{}.'.format(premium, position_df))
            return _place_single_order(asset, backend_setting,
                    quantity=-position_df['quantity'],
                    contract_df=contract_df,
                    )
    #END: __closing_cb
    _scan_and_adjust(asset, option_chain, backend_setting, __closing_cb)
#END: probe

#################
# LOW-LEVEL API #
#################

def _close_profitable_longs(asset, option_chain, backend_setting, target_type=None, profit_target=1.0):
    """
    Close profitable/breakeven long contracts of specified type.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain: pd.DataFrame
        Option chain.

    backend_setting: dict
        Backend setting.

    target_type: autoassets.OptionContractType (default: any contract type)
        Type of contract to close.

    profit_target: float (default: 1.0, breakeven after slippage)
        Fraction of cost per contract at which it is closed.
    """
    if 'leg_db' not in asset:
        return
    positioning = asset['definition']['positioning']
    def __closing_cb(leg_df, side_df, contract_df, position_df):
        contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
        if (target_type is not None and contract_type != target_type) or position_df['quantity'] <= 0: # Wrong contract type or not a long contract.
            return False
        cum_quantity_df = _coverage(side_df)
        coverage = int(cum_quantity_df.loc[contract_df.name]['quantity'])
        cost_per_unit = (position_df['cost'] / (position_df['quantity'] * position_df['shares_per_contract']))
        premium_per_unit = contract_df[autoassets.OptionContractField.BID_PRICE] * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - position_df['quantity'] * (COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT)
        target_premium_per_unit = profit_target * cost_per_unit
        if coverage > 0 and target_premium_per_unit <= premium_per_unit: # Contract is profitable; close it.
            logger.info('Closing profitable {} contract; cost_per_unit={}, target_premium_per_unit={}, premium_per_unit={}:\n{}.'.format(target_type, cost_per_unit, target_premium_per_unit, premium_per_unit, position_df))
            adjusted = False
            if 'hedge_symbol' in position_df and position_df['hedge_symbol'] != 'nan':
                leg_df, call_df, put_df = _leg_dataframe(asset)
                hedge_symbol = position_df['hedge_symbol']
                hedge_position_df = None if hedge_symbol not in leg_df.index else leg_df.loc[hedge_symbol]
                hedge_df = option_chain.loc[hedge_symbol]
                if _place_single_order(asset, backend_setting,
                        quantity=position_df['quantity'],
                        contract_specs=(hedge_df, hedge_position_df),
                        ):
                    adjusted = True
            return adjusted or _place_single_order(asset, backend_setting,
                    quantity=-position_df['quantity'],
                    contract_df=contract_df,
                    )
        return False
    #END: _closing_cb
    _scan_and_adjust(asset, option_chain, backend_setting, __closing_cb, sort_by='strike', ascending=False)
#END: _close_profitable_longs

def _close_profitable_or_uncovered_shorts(asset, option_chain, backend_setting, target_type=None, profit_target=1.0, loss_limit=2.0):
    """
    Close profitable/breakeven or uncovered short contracts of specified type.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain: pd.DataFrame
        Option chain.

    backend_setting: dict
        Backend setting.

    target_type: autoassets.OptionContractType (default: any contract type)
        Type of contract to close.

    profit_target: float (default: 1.0, breakeven after slippage)
        Fraction of cost per contract at which any short contract is closed.

    loss_limit: float (default: 2.0, twice premium received)
        Fraction of cost per contract at which uncovered short contracts are closed.
    """
    if 'leg_db' not in asset:
        return
    positioning = asset['definition']['positioning']
    def __closing_cb(leg_df, side_df, contract_df, position_df):
        contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
        if (target_type is not None and contract_type != target_type) or position_df['quantity'] >= 0: # Wrong contract type or not a short contract.
            return False
        cum_quantity_df = _coverage(side_df)
        coverage = int(cum_quantity_df.loc[contract_df.name]['quantity'])
        cost_per_unit = (position_df['cost'] / (position_df['quantity'] * position_df['shares_per_contract']))
        premium_per_unit = contract_df[autoassets.OptionContractField.BID_PRICE] * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - abs(position_df['quantity']) * (COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT)
        target_premium_per_unit = max(MAX_BUY_MARGIN_PREMIUM, profit_target * cost_per_unit)
        loss_premium_per_unit = loss_limit * cost_per_unit
        if target_premium_per_unit >= premium_per_unit or (coverage < 0 and loss_premium_per_unit <= premium_per_unit): # Contract is profitable; close it.
            target_quantity = abs(position_df['quantity']) if target_premium_per_unit >= premium_per_unit else abs(coverage)
            logger.info('Closing {} contract; coverage={}, cost_per_unit={}, target_quantity={}, target_premium_per_unit={}, loss_premium_per_unit={}, premium_per_unit={}:\n{}.'.format(target_type, coverage, cost_per_unit, target_quantity, target_premium_per_unit, loss_premium_per_unit, premium_per_unit, position_df))
            return  _place_single_order(asset, backend_setting,
                    quantity=target_quantity,
                    contract_specs=(contract_df, position_df),
                    )
        return False
    #END: _closing_cb
    _scan_and_adjust(asset, option_chain, backend_setting, __closing_cb, sort_by='strike', ascending=False)
#END: _close_profitable_or_uncovered_shorts

def _roll_profitable_longs(asset, option_chain, backend_setting, target_type=None):
    """
    Roll profitable/breakeven contracts of specified bias.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain: pd.DataFrame
        Option chain.

    backend_setting: dict
        Backend setting.

    target_type: autoassets.OptionContractType (default: any contract type)
        Type of contract to close.
    """
    if 'leg_db' not in asset:
        return
    positioning = asset['definition']['positioning']
    enable_trades = False if 'enable_trades' not in positioning else positioning['enable_trades']
    def __adjustment_cb(leg_df, side_df, contract_df, position_df):
        contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
        if (target_type is not None and contract_type != target_type) or position_df['quantity'] <= 0: # Wrong contract type or not a long contract.
            return False
        cost_per_unit = (position_df['cost'] / (position_df['quantity'] * position_df['shares_per_contract']))
        opex = position_df['opex']
        target_premium_per_unit = 2.0 * cost_per_unit
        premium_per_unit = contract_df[autoassets.OptionContractField.BID_PRICE] * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - (COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT)
        if target_premium_per_unit <= premium_per_unit: # Contract is profitable; roll it.
            logger.info('Rolling profitable {} contract; cost_per_unit={}, target_premium_per_unit={}, premium_per_unit={}:\n{}.'.format(target_type, cost_per_unit, target_premium_per_unit, premium_per_unit, position_df))
            target_ask_per_unit = cost_per_unit * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - (COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT)
            sell_specs = (contract_df, position_df)
            if contract_type == autoassets.OptionContractType.CALL:
                new_contract_filter = option_chain[
                    (option_chain[autoassets.OptionContractField.OPEX] == opex) &
                    (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == contract_type) &
                    (option_chain[autoassets.OptionContractField.DELTA] >= MIN_LONG_DELTA) &
                    (option_chain[autoassets.OptionContractField.ASK_PRICE] <= target_ask_per_unit)
                    ]
            elif contract_type == autoassets.OptionContractType.PUT:
                new_contract_filter = option_chain[
                    (option_chain[autoassets.OptionContractField.OPEX] == opex) &
                    (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == contract_type) &
                    (option_chain[autoassets.OptionContractField.DELTA] <= -MIN_LONG_DELTA) &
                    (option_chain[autoassets.OptionContractField.ASK_PRICE] <= target_ask_per_unit)
                    ]
            if len(new_contract_filter) == 0:
                logger.info('Abort roll: Cannot find target contract. Closing old contract instead.')
                return _place_single_order(asset, backend_setting,
                        quantity=-position_df['quantity'],
                        contract_specs=sell_specs,
                        )
            if contract_type == autoassets.OptionContractType.CALL:
                new_contract_symbol = new_contract_filter[autoassets.OptionContractField.STRIKE].idxmin()
            elif contract_type == autoassets.OptionContractType.PUT:
                new_contract_symbol = new_contract_filter[autoassets.OptionContractField.STRIKE].idxmax()
            new_contract_df = option_chain.loc[new_contract_symbol]
            new_position_df = None if new_contract_symbol not in leg_df.index else leg_df.loc[new_contract_symbol]
            buy_specs = (new_contract_df, new_position_df)
            return _place_spread_order(asset, backend_setting,
                    quantity=position_df['quantity'],
                    buy_specs=buy_specs,
                    sell_specs=sell_specs,
                    )
        #else:
        #    logger.debug('Trying to close {} contract; cost_per_unit={}, target_premium_per_unit={}, premium_per_unit={}:\n{}.'.format(target_bias, cost_per_unit, target_premium_per_unit, premium_per_unit, position_df))
        return False
    #END: __adjustment_cb
    _scan_and_adjust(asset, option_chain, backend_setting, __adjustment_cb)
#END: _roll_profitable_longs

def _coverage(side_df):
    """
    Calculate coverage of positions.

    Parameters
    ----------
    side_df: pd.Dataframe
        Leg dataframe of respective side indexed by contract symbol.

    Returns
    -------
    pd.Dataframe:
        Cumulative sum of quantity.
    """
    contract_type = side_df['contract_type'].iloc[0]
    ascending = (contract_type == autoassets.OptionContractType.CALL)
    side_filter = side_df.sort_values(by=['strike'], ascending=ascending)
    return side_filter[['quantity']].cumsum()
#END: _coverage

def _target_delta(mark, key_levels, bullish_bias):
    """
    Calculate target delta from market and key levels.

    Parameters
    ----------
    mark: float
        Current market value.

    key_levels: list
        Key price levels.

    bullish_bias: bool
        True if bullish, False if bearish.

    Returns
    -------
    float:
        Target delta.
    """
    #return (key_levels[3] - mark) / (4.0 * (key_levels[1] - key_levels[0]))
    lower, upper = _target_key_range(mark, key_levels)
    logger.debug('target key range = [{}, {}].'.format(lower, upper))
    if upper <= key_levels[0]:
        return MAX_LONG_DELTA if bullish_bias else 0.0
    if lower >= key_levels[-1]:
        return -MAX_LONG_DELTA if not bullish_bias else 0.0
    if bullish_bias:
        return MAX_LONG_DELTA * (upper - mark) / (upper - lower)
    else:
        return -MAX_LONG_DELTA * (mark - lower) / (upper - lower)
#END: _target_delta

def _target_key_level(mark, key_levels, bias=0):
    """
    Calculate target key level from market and key levels.

    Parameters
    ----------
    mark: float
        Current market value.

    key_levels: list
        Key price levels.

    bias: int (default: 0)
        Number specifying direction and magnitude in selection. Zero selects nearest key level.

    Returns
    -------
    float:
        Target key level.
    """
    idx, nearest = min(enumerate(key_levels), key=lambda x:abs(x[1]-mark))
    if bias > 0 and mark < nearest:
        idx -= 1
    elif bias < 0 and mark > nearest:
        idx += 1
    select_idx = max(0, min(len(key_levels) - 1, idx + bias))
    return key_levels[select_idx]
#END: _target_key_level

def _target_key_range(mark, key_levels):
    """
    Calculate target key range from market and key levels.

    Parameters
    ----------
    mark: float
        Current market value.

    key_levels: list
        Key price levels.

    Returns
    -------
    (float, float):
        Target key range (lower, upper).
    """
    idx, nearest = min(enumerate(key_levels), key=lambda x:abs(x[1]-mark))
    if mark < nearest and idx > 0:
        idx -= 1
    other_idx = min(len(key_levels) - 1, idx + 1)
    return (key_levels[idx], key_levels[other_idx])
#END: _target_key_range

def _init_leg(asset, leg):
    """
    Initialize given leg in asset.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    leg: pd.Series
        Leg specifications.
    """
    if 'leg_db' not in asset:
        asset['leg_db'] = {}
    asset['leg_db'][leg.name] = {
        'opex': leg[autoassets.OptionContractField.OPEX].isoformat(),
        'strike': leg[autoassets.OptionContractField.STRIKE],
        'contract_type': leg[autoassets.OptionContractField.CONTRACT_TYPE].value,
        'shares_per_contract': UNITS_PER_CONTRACT,
        'created_at': pd.to_datetime('now', utc=True).isoformat(),
        'cost': 0.0,
        'quantity': 0,
    }
#END: _init_leg

def _init_shadow_leg(asset, leg):
    """
    Initialize given shadow leg in asset.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    leg: pd.Series
        Leg specifications.
    """
    if 'shadow_leg_db' not in asset:
        asset['shadow_leg_db'] = {}
    asset['shadow_leg_db'][leg.name] = {
        'opex': leg[autoassets.OptionContractField.OPEX].isoformat(),
        'strike': leg[autoassets.OptionContractField.STRIKE],
        'contract_type': leg[autoassets.OptionContractField.CONTRACT_TYPE].value,
        'shares_per_contract': UNITS_PER_CONTRACT,
        'cost': 0.0,
        'quantity': 0,
        'maximum_price': 0.0,
    }
#END: _init_shadow_leg

def _leg_dataframe(asset):
    """
    Typecast asset's legs to pd.DataFrame.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        Leg dataframe indexed by contract symbol, call dataframe and put dataframe.
    """
    leg_df = pd.DataFrame.from_dict({} if 'leg_db' not in asset else asset['leg_db'],
        orient='index',
        columns=[
            'opex',
            'strike',
            'contract_type',
            'shares_per_contract',
            'created_at',
            'cost',
            'quantity',
            'last_trade_price',
        ])
    leg_df.index.rename('symbol', inplace=True)
    leg_df['opex'] = pd.to_datetime(leg_df['opex'])
    leg_df['created_at'] = pd.to_datetime(leg_df['created_at'])
    leg_df['contract_type'] = leg_df['contract_type'].apply(autoassets.OptionContractType)
    call_df = leg_df[leg_df['contract_type'] == autoassets.OptionContractType.CALL]
    put_df = leg_df[leg_df['contract_type'] == autoassets.OptionContractType.PUT]
    return (leg_df, call_df, put_df)
#END: _leg_dataframe

def _shadow_leg_dataframe(asset):
    """
    Typecast asset's shadow legs to pd.DataFrame.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        Shadow leg dataframe indexed by contract symbol, call dataframe and put dataframe.
    """
    leg_df = pd.DataFrame.from_dict({} if 'shadow_leg_db' not in asset else asset['shadow_leg_db'],
        orient='index',
        columns=[
            'opex',
            'strike',
            'contract_type',
            'shares_per_contract',
            'cost',
            'quantity',
            'maximum_price',
        ])
    leg_df.index.rename('symbol', inplace=True)
    leg_df['opex'] = pd.to_datetime(leg_df['opex'])
    leg_df['contract_type'] = leg_df['contract_type'].apply(autoassets.OptionContractType)
    call_df = leg_df[leg_df['contract_type'] == autoassets.OptionContractType.CALL]
    put_df = leg_df[leg_df['contract_type'] == autoassets.OptionContractType.PUT]
    return (leg_df, call_df, put_df)
#END: _shadow_leg_dataframe

def _maybe_place_single_order(asset, backend_setting, quantity, contract_df):
    """
    Place a single-legged order if it fits the asset's budget and improves current position.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    backend_setting: dict
        Backend setting.

    quantity: int
        Number of spreads to trade.

    contract_df: pd.Series
        Contract to trade.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
    if contract_type == autoassets.OptionContractType.UNSUPPORTED:
        logger.debug('Abort trade: Unsupported contract type {}.'.format(contract_df))
        return False
    positioning = asset['definition']['positioning']
    leg_df, call_df, put_df = _leg_dataframe(asset)
    symbol = contract_df[autoassets.OptionContractField.SYMBOL]
    logger.debug('Candidate single: {}.'.format(symbol))
    if quantity > 0: # Buy order.
        candidate_premium_per_unit = contract_df[autoassets.OptionContractField.ASK_PRICE]
        slippage = candidate_premium_per_unit - contract_df[autoassets.OptionContractField.BID_PRICE]
    elif quantity < 0: # Sell order.
        candidate_premium_per_unit = contract_df[autoassets.OptionContractField.BID_PRICE]
        slippage = contract_df[autoassets.OptionContractField.ASK_PRICE] - candidate_premium_per_unit
    else: # No order.
        logger.debug('BUG: Abort trade: Zero budget.')
        return False
    # Recall respective leg (if any) in asset.
    position_df = None if symbol not in leg_df.index else leg_df.loc[symbol]
    # Check against last trade.
    if position_df is not None:
        logger.debug('Abort trade: Already have position.')
        return False
    #if position_df is not None and 'last_trade_price' in position_df:
    #    last_premium_per_unit = position_df['last_trade_price']
    #    maximum_premium_per_unit = last_premium_per_unit * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - abs(last_premium_per_unit / 2.0) - (slippage + (COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT))
    #    if candidate_premium_per_unit >= maximum_premium_per_unit: # Want better premium for single.
    #        logger.debug('Abort trade: Candidate premium/contract, {}, exceeds maximum premium/contract, {} (last premium/contract = {}).'.format(candidate_premium_per_unit, maximum_premium_per_unit, last_premium_per_unit))
    #        return False
    #contract_specs = (contract_df, position_df)
    return _place_single_order(asset, backend_setting,
            quantity=quantity,
            contract_df=contract_df,
            )
#END: _maybe_place_single_order

def _maybe_place_spread_order(asset, backend_setting, quantity, buy_df, sell_df):
    """
    Place a spread (two-legged) order if it improves current position, if any.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    backend_setting: dict
        Backend setting.

    quantity: int
        Number of spreads to trade.

    buy_df: pd.Series
        Contract to buy.

    sell_df: pd.Series
        Contract to sell.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    if buy_df.equals(sell_df):
        logger.debug('Abort trade: Zero spread.')
        return False
    contract_type = buy_df[autoassets.OptionContractField.CONTRACT_TYPE]
    if contract_type != sell_df[autoassets.OptionContractField.CONTRACT_TYPE] or contract_type == autoassets.OptionContractType.UNSUPPORTED:
        logger.debug('Abort trade: Mismatched contract types between long {} and short {}.'.format(buy_df, sell_df))
        return False
    positioning = asset['definition']['positioning']
    last_trade_alpha = 2.0 if 'last_trade_alpha' not in positioning else positioning['last_trade_alpha']
    leg_df, call_df, put_df = _leg_dataframe(asset)
    long_symbol = buy_df[autoassets.OptionContractField.SYMBOL]
    short_symbol = sell_df[autoassets.OptionContractField.SYMBOL]
    logger.debug('Candidate spread: buy {}, sell {}'.format(long_symbol, short_symbol))
    long_premium = buy_df[autoassets.OptionContractField.ASK_PRICE]
    short_premium = sell_df[autoassets.OptionContractField.BID_PRICE]
    long_slippage = long_premium - buy_df[autoassets.OptionContractField.BID_PRICE]
    short_slippage = sell_df[autoassets.OptionContractField.ASK_PRICE] - short_premium
    long_strike = buy_df[autoassets.OptionContractField.STRIKE]
    short_strike = sell_df[autoassets.OptionContractField.STRIKE]
    # Recall respective legs (if any) in asset.
    long_position_df = None if long_symbol not in leg_df.index else leg_df.loc[long_symbol]
    short_position_df = None if short_symbol not in leg_df.index else leg_df.loc[short_symbol]
    # Check short-leg against last trade.
    if short_position_df is not None:
        logger.debug('Abort trade: Already have position.')
        return False
    #if short_position_df is not None and 'last_trade_price' in short_position_df:
    #    last_premium_per_unit = short_position_df['last_trade_price']
    ##    #lower_premium_per_unit = ((2.0 - (last_trade_alpha + 2.0 * NONIDEAL_ADJUSTMENT_FRACTION)) * last_premium_per_unit) - (long_slippage + short_slippage + (2.0 * COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT))
    #    upper_premium_per_unit = ((last_trade_alpha + 2.0 * NONIDEAL_ADJUSTMENT_FRACTION) * last_premium_per_unit) + (long_slippage + short_slippage + (2.0 * COMMISSION_PER_CONTRACT / UNITS_PER_CONTRACT))
    ##    #if short_premium > lower_premium_per_unit and short_premium < upper_premium_per_unit: # Want different premium from last trade.
    ##    #    logger.debug('Abort trade: Candidate premium/unit, {}, unchanged from last trade range ([{}, {}]; (last premium/spread = {}).'.format(short_premium, lower_premium_per_unit, upper_premium_per_unit, last_premium_per_unit))
    #    if short_premium < upper_premium_per_unit: # Want higher short-premium from last trade.
    ##    if True:
    #        logger.debug('Abort trade: Candidate premium/unit, {}, less than required, {} (last premium/spread = {}).'.format(short_premium, upper_premium_per_unit, last_premium_per_unit))
    ##        logger.debug('Abort trade: Already have short position in {}.'.format(short_symbol))
    ##        return False
    ## Reuse existing long contracts when possible.
    #df = call_df if contract_type == autoassets.OptionContractType.CALL else put_df
    #sell_specs = (sell_df, short_position_df)
    #if df['quantity'].sum() >= quantity: # There exists sufficient unused long contracts.
    #    return _place_single_order(asset, backend_setting,
    #            quantity=-quantity,
    #            contract_specs=sell_specs,
    #            )
    #else:
    #    buy_specs = (buy_df, long_position_df)
    #    return _place_spread_order(asset, backend_setting,
    #            quantity=quantity,
    #            buy_specs=buy_specs,
    #            sell_specs=sell_specs,
    #            )
    #buy_specs = (buy_df, long_position_df)
    return _place_spread_order(asset, backend_setting,
            quantity=quantity,
            buy_specs=buy_df,
            sell_specs=sell_df,
            )
#END: _maybe_place_spread_order

def _place_single_order(asset, backend_setting, quantity, contract_df, hedged_symbol=None):
    """
    Place a single-legged order.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    backend_setting: dict
        Backend setting.

    quantity: int
        Number of contracts to buy if positive, or sell if negative.

    contract_df: pd.Series
        Contract to trade.

    hedged_symbol: str (optional)
        Contract symbol that this order is hedging.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    positioning = asset['definition']['positioning']
    enable_trades = False if 'enable_trades' not in positioning else positioning['enable_trades']
    leg_df, _, _ = _leg_dataframe(asset)
    symbol = contract_df[autoassets.OptionContractField.SYMBOL]
    position_df = None if symbol not in leg_df.index else leg_df.loc[symbol]
    #contract_df, position_df = contract_specs
    if quantity > 0: # Buy order.
        direction = autoassets.OrderDirection.BUY_TO_OPEN if position_df is None or position_df['quantity'] > 0 else autoassets.OrderDirection.BUY_TO_CLOSE
        premium = contract_df[autoassets.OptionContractField.ASK_PRICE]
        other_premium = contract_df[autoassets.OptionContractField.BID_PRICE]
    elif quantity < 0: # Sell order.
        direction = autoassets.OrderDirection.SELL_TO_OPEN if position_df is None or position_df['quantity'] < 0 else autoassets.OrderDirection.SELL_TO_CLOSE
        premium = contract_df[autoassets.OptionContractField.BID_PRICE]
        other_premium = contract_df[autoassets.OptionContractField.ASK_PRICE]
    else: # No order.
        return False
    contract_type = contract_df[autoassets.OptionContractField.CONTRACT_TYPE]
    strike = contract_df[autoassets.OptionContractField.STRIKE]
    # Setup per-leg direction and place spread order.
    if enable_trades and (quantity > 0 or premium >= MIN_SELL_PREMIUM):
        backend = positioning['backend']
        instrument_type = positioning['instrument_type']
        account_id = backend_setting['account_id']
        if not backend.place_market_order(account_id, instrument_type, symbol, direction, abs(quantity)):
            return False
    # Update last trade.
    if 'leg_db' not in asset:
        asset['leg_db'] = {}
    if 'profit' not in asset:
        asset['profit'] = 0.0
    # Update leg.
    if symbol not in asset['leg_db']:
        _init_leg(asset, contract_df)
    asset_contract = asset['leg_db'][symbol]
    if hedged_symbol is not None and hedged_symbol in asset['leg_db']:
        asset['leg_db'][hedged_symbol]['hedge_symbol'] = symbol
    # Calculate profit, if closing contracts, and new cost.
    profit = 0.0
    cost = 0.0
    if direction == autoassets.OrderDirection.BUY_TO_CLOSE or direction == autoassets.OrderDirection.SELL_TO_CLOSE:
        closing_units = position_df['shares_per_contract'] * min(abs(quantity), abs(position_df['quantity']))
        profit = closing_units * (
                math.copysign(1.0, quantity) * (
                    (position_df['cost'] / (position_df['quantity'] * position_df['shares_per_contract'])) -
                    premium * (1.0 + math.copysign(1.0, quantity) * NONIDEAL_ADJUSTMENT_FRACTION)
                ) -
                (COMMISSION_PER_CONTRACT / position_df['shares_per_contract'])
                )
    asset_contract['quantity'] += quantity
    if asset_contract['quantity'] == 0:
        del asset['leg_db'][symbol]
    else: # Non-zero position.
        asset_contract['last_trade_price'] = premium
        cost = profit + quantity * (
                UNITS_PER_CONTRACT * premium * (1.0 + math.copysign(1.0, quantity) * NONIDEAL_ADJUSTMENT_FRACTION) +
                math.copysign(1.0, quantity) * COMMISSION_PER_CONTRACT
            )
        asset_contract['cost'] += cost
    # Cleanup
    log = 'Traded {}; {} quantity: premium = {}; cost = {}: profit = {}.'.format(symbol, quantity, premium, cost, profit)
    if not enable_trades:
        logger.debug('Simulated trade: {}'.format(log))
    else:
        logger.info(log)
    asset['profit'] += profit
    asset['slippage'] += ((abs(premium * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION) - other_premium) / 2.0) * UNITS_PER_CONTRACT + COMMISSION_PER_CONTRACT) * abs(quantity)
    if len(asset['leg_db']) == 0:
        del asset['leg_db']
    return True
#END: _place_single_order

def _place_spread_order(asset, backend_setting, quantity, buy_df, sell_df):
    """
    Place a spread (two-legged) order.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    backend_setting: dict
        Backend setting.

    quantity: int
        Number of spreads to trade.

    buy_df: pd.Series
        Contract to buy.

    sell_df: pd.Series
        Contract to sell.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    positioning = asset['definition']['positioning']
    enable_trades = False if 'enable_trades' not in positioning else positioning['enable_trades']
    leg_df, _, _ = _leg_dataframe(asset)
    long_symbol = buy_df[autoassets.OptionContractField.SYMBOL]
    short_symbol = sell_df[autoassets.OptionContractField.SYMBOL]
    long_position_df = None if long_symbol not in leg_df.index else leg_df.loc[long_symbol]
    short_position_df = None if short_symbol not in leg_df.index else leg_df.loc[short_symbol]
    #buy_df, long_position_df = buy_specs
    #sell_df, short_position_df = sell_specs
    contract_type = buy_df[autoassets.OptionContractField.CONTRACT_TYPE]
    long_premium = buy_df[autoassets.OptionContractField.ASK_PRICE]
    other_long_premium = buy_df[autoassets.OptionContractField.BID_PRICE]
    short_premium = sell_df[autoassets.OptionContractField.BID_PRICE]
    other_short_premium = sell_df[autoassets.OptionContractField.ASK_PRICE]
    # Setup per-leg direction and place spread order.
    open_or_close_long = autoassets.OrderDirection.BUY_TO_OPEN if long_position_df is None or long_position_df['quantity'] > 0 else autoassets.OrderDirection.BUY_TO_CLOSE
    open_or_close_short = autoassets.OrderDirection.SELL_TO_OPEN if short_position_df is None or short_position_df['quantity'] < 0 else autoassets.OrderDirection.SELL_TO_CLOSE
    leg_orders=[
        {
            'symbol': long_symbol,
            'direction': open_or_close_long,
            'quantity': quantity,
        },
        {
            'symbol': short_symbol,
            'direction': open_or_close_short,
            'quantity': quantity,
        },
    ]
    if enable_trades and short_premium >= MIN_SELL_PREMIUM:
        backend = positioning['backend']
        instrument_type = positioning['instrument_type']
        account_id = backend_setting['account_id']
        if not backend.place_multi_leg_market_order(account_id, instrument_type, leg_orders):
            return False
    # Update last trade.
    if 'leg_db' not in asset:
        asset['leg_db'] = {}
    if 'profit' not in asset:
        asset['profit'] = 0.0
    spread_profit = 0.0
    # Update long leg.
    if long_symbol not in asset['leg_db']:
        _init_leg(asset, buy_df)
    asset_long = asset['leg_db'][long_symbol]
    # Calculate profit, if closing contracts, and new cost.
    long_profit = 0.0
    long_cost = 0.0
    if open_or_close_long == autoassets.OrderDirection.BUY_TO_CLOSE:
        closing_units = long_position_df['shares_per_contract'] * min(quantity, abs(long_position_df['quantity']))
        long_profit = closing_units * (
                (long_position_df['cost'] / (long_position_df['quantity'] * long_position_df['shares_per_contract'])) -
                long_premium * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION) -
                (COMMISSION_PER_CONTRACT / long_position_df['shares_per_contract'])
                )
    spread_profit += long_profit
    asset_long['quantity'] += quantity
    if asset_long['quantity'] == 0:
        del asset['leg_db'][long_symbol]
    else: # Non-zero position.
        asset_long['last_trade_price'] = long_premium
        long_cost = long_profit + quantity * (UNITS_PER_CONTRACT * (long_premium * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION)) + COMMISSION_PER_CONTRACT)
        asset_long['cost'] += long_cost
    # Update short leg.
    if short_symbol not in asset['leg_db']:
        _init_leg(asset, sell_df)
    asset_short = asset['leg_db'][short_symbol]
    # Calculate profit, if closing contracts, and new cost.
    short_profit = 0.0
    short_cost = 0.0
    if open_or_close_short == autoassets.OrderDirection.SELL_TO_CLOSE:
        closing_units = short_position_df['shares_per_contract'] * min(quantity, short_position_df['quantity'])
        short_profit = closing_units * (
                short_premium * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) -
                (short_position_df['cost'] / (short_position_df['quantity'] * short_position_df['shares_per_contract'])) -
                (COMMISSION_PER_CONTRACT / short_position_df['shares_per_contract'])
                )
    spread_profit += short_profit
    asset_short['quantity'] -= quantity
    if asset_short['quantity'] == 0:
        del asset['leg_db'][short_symbol]
    else: # Non-zero position.
        asset_short['last_trade_price'] = short_premium
        short_cost = short_profit + quantity * (UNITS_PER_CONTRACT * (-short_premium * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION)) + COMMISSION_PER_CONTRACT)
        asset_short['cost'] += short_cost
    # Cleanup
    log = 'Bought {} - Sold {}; {} quantity: long premium = {}, short premium = {}, spread premium = {}; spread cost = {}: long profit = {}, short profit = {}, spread profit = {}.'.format(long_symbol, short_symbol, quantity, long_premium, short_premium, (long_premium - short_premium), (long_cost + short_cost), long_profit, short_profit, spread_profit)
    if not enable_trades:
        logger.debug('Simulated trade: {}'.format(log))
    else:
        logger.info(log)
    asset['profit'] += spread_profit
    asset['slippage'] += ((abs(long_premium * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION) - other_long_premium) / 2.0 + abs(short_premium * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION) - other_short_premium) / 2.0) * UNITS_PER_CONTRACT + 2.0 * COMMISSION_PER_CONTRACT) * quantity
    if len(asset['leg_db']) == 0:
        del asset['leg_db']
    return True
#END: _place_spread_order

def _scan_and_adjust(asset, option_chain, backend_setting, adjustment_cb, sort_by='quantity', ascending=True, scan_shadow_db=False):
    """
    Scan and adjust contracts according to callback function.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain: pd.DataFrame
        Option chain.

    backend_setting: dict
        Backend setting.

    adjustment_cb: function(leg_df, side_df, contract_df, position_df)
        Callback that adjusts a give position.
            Parameters
            ----------
            leg_df: pd.Dataframe
                Leg dataframe indexed by contract symbol.

            side_df: pd.Dataframe
                Leg dataframe of respective side indexed by contract symbol.

            contract_df: pd.Series
                Contract information including its current quote.

            position_df: pd.Series
                Position information.

            Returns
            -------
                bool:
                    Whether or not an adjustment was made.

    sort_by: str (default: 'quantity')
        What field to sort positions by.

    ascending: bool (default: True)
        Whether to sort positions in ascending (True) or descending (False) order.

    scan_shadow_db: bool (default: False)
        Whether to scan primary leg database (False) or shadow leg database (True).
    """
    if scan_shadow_db:
        leg_df, call_df, put_df = _shadow_leg_dataframe(asset)
    else:
        leg_df, call_df, put_df = _leg_dataframe(asset)
    positioning = asset['definition']['positioning']
    for side_df in (call_df, put_df): # Scan call- and put-side separately.
        while True:
            if len(side_df) < 1: # No contracts on this side.
                break
            contract_df = side_df.sort_values(by=sort_by, ascending=ascending)
            contract_adjusted = False
            for symbol, position_df in contract_df.iterrows():
                contract_type = position_df['contract_type']
                if contract_type == autoassets.OptionContractType.UNSUPPORTED: # Should never happen; otherwise report bug.
                    logger.error('BUG: Unsupported contract type {}.'.format(position_df))
                    return False
                contract = option_chain.loc[symbol]
                contract_adjusted = adjustment_cb(leg_df, side_df, contract, position_df)
                if contract_adjusted: # Update side dataframe, break out of leg loop.
                    if scan_shadow_db:
                        leg_df, call_df, put_df = _shadow_leg_dataframe(asset)
                    else:
                        leg_df, call_df, put_df = _leg_dataframe(asset)
                    side_df = call_df if contract_type == autoassets.OptionContractType.CALL else put_df
                    break
            if not contract_adjusted: # No contracts adjusted on this side; continue to next side.
                break
#END: _scan_and_adjust

def _total_cost(asset):
    """
    Calculate total cost, maximum risk, of asset.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    float:
        Total cost of asset.
    """
    if 'profit' not in asset:
        asset['profit'] = 0.0
    if 'slippage' not in asset:
        asset['slippage'] = 0.0
    leg_df, call_df, put_df = _leg_dataframe(asset)
    if call_df['quantity'].sum() < 0: # At least one uncovered short-call exists.
        logger.error('Undefined risk asset: {}.'.format(asset))
        return math.inf
    max_call_strike = call_df['strike'].max()
    # Put-side liability.
    put_short_df = put_df[put_df['quantity'] < 0]
    put_short_liability = (put_short_df['strike'] * put_short_df['shares_per_contract'] * (abs(put_short_df['quantity']))).sum()
    put_short_quantity = abs(put_short_df['quantity']).sum()
    put_long_df = put_df[put_df['quantity'] > 0].sort_values(by=['strike'], ascending=False)
    i = put_short_quantity
    put_idx = 0
    put_liability = put_short_liability
    while i > 0 and put_idx < len(put_long_df) and put_liability >= 0.0:
        put = put_long_df.iloc[put_idx]
        q = min(i, put['quantity'])
        put_liability -= q * put['strike'] * put['shares_per_contract']
        put_idx += 1
        i -= q
    # Call-side liability.
    call_short_df = call_df[call_df['quantity'] < 0]
    call_short_liability = ((max_call_strike - call_short_df['strike']) * call_short_df['shares_per_contract'] * (abs(call_short_df['quantity']))).sum()
    call_short_quantity = abs(call_short_df['quantity']).sum()
    call_long_df = call_df[call_df['quantity'] > 0].sort_values(by=['strike'], ascending=True)
    i = call_short_quantity
    call_idx = 0
    call_liability = call_short_liability
    while i > 0 and call_idx < len(call_long_df) and call_liability >= 0.0:
        call = call_long_df.iloc[call_idx]
        q = min(i, call['quantity'])
        call_liability -= q * (max_call_strike - call['strike']) * call['shares_per_contract']
        call_idx += 1
        i -= q
    return (
            # Premium spent less received.
            leg_df['cost'].sum() +
            # Liabilities.
            max(0.0, call_liability, put_liability)
           )
#END: _total_cost

def _total_profit(asset, option_chain):
    """
    Calculate total running profit of asset given current market value of positions.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    option_chain: pd.DataFrame
        Option chain.

    Returns
    -------
    (float, float):
        Market value and total running profit of asset.
    """
    if 'profit' not in asset:
        asset['profit'] = 0.0
    if 'slippage' not in asset:
        asset['slippage'] = 0.0
    leg_df, call_df, put_df = _leg_dataframe(asset)
    market_value = 0.0
    for symbol, position_df in leg_df.iterrows():
        #if symbol not in option_chain.:
        #    continue
        contract = option_chain.loc[symbol]
        if position_df['quantity'] < 0:
            contract_premium = contract[autoassets.OptionContractField.ASK_PRICE] * (1.0 + NONIDEAL_ADJUSTMENT_FRACTION) + (COMMISSION_PER_CONTRACT / position_df['shares_per_contract'])
        else:
            contract_premium = contract[autoassets.OptionContractField.BID_PRICE] * (1.0 - NONIDEAL_ADJUSTMENT_FRACTION) - (COMMISSION_PER_CONTRACT / position_df['shares_per_contract'])
        market_value += contract_premium * position_df['quantity'] * position_df['shares_per_contract']
    return (
            market_value,
            # Market value.
            market_value +
            # Premium received less spent.
            -leg_df['cost'].sum() +
            # Realized profit.
            asset['profit']
           )
#END: _total_profit
