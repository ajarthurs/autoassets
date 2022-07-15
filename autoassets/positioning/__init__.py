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
Position management.
"""

import autoassets
import autoassets.schedule
import logging
import math

logger = logging.getLogger(__name__)

MIN_PIVOT_INTEREST_FRACTION = 0.68 # fraction of total interest to 

def active_instruments(assets, require_optionable=False):
    """
    Return a unique list of instruments.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    require_optionable: bool (default: False)
        Return optionable instruments only.

    Returns
    -------
    [dict]:
        Unique list of active instruments.
    """
    active_instruments = []
    for asset in assets:
        if not autoassets.asset_active(asset):
            continue
        positioning = asset['definition']['positioning']
        if require_optionable and not('optionable' in positioning and positioning['optionable']):
            continue
        structure = positioning['structure']
        instruments = structure.instruments(positioning, require_optionable)
        active_instruments += instruments
    return list(map(dict, frozenset([frozenset(i.items()) for i in active_instruments])))
#END: active_instruments

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
    (int, int)
        Units available to buy and sell, in that order.
    """
    positioning = asset['definition']['positioning']
    return positioning['structure'].availability(asset, quote_db, option_chain_db)
#END: availability

def backend(positioning):
    """
    Return backend.

    Parameters
    ----------
    positioning: dict
        Position specifications.

    Returns
    -------
    module:
        Backend module (see autoassets.backend).
    """
    return positioning['backend']
#END: backend

def cb_run_probes(instruments, data):
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
        if not autoassets.schedule.is_now_tradable():
            continue
        asset['definition']['positioning']['structure'].probe(asset, instrument_db, option_chain_db, quote_db, backend_setting)
#END: cb_run_probes

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
    positioning = asset['definition']['positioning']
    structure = positioning['structure']
    return structure.cost_per_unit(asset, option_chain_db)
#END: cost_per_unit

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
    positioning = asset['definition']['positioning']
    structure = positioning['structure']
    return structure.delta(asset, option_chain_db)
#END: delta

def get_cpivot(option_chain):
    """
    Return call-side pivot, if any.

    Parameters
    ----------
    option_chain: pd.DataFrame
        Option chain.

    Returns
    -------
    pd.Series:
        Call-side pivot contract.
    """
    # Calculate cumulative sum of open interest and volume per strike on the call side.
    call_filter = option_chain[
        (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.CALL)
        ].sort_values(by=[autoassets.OptionContractField.STRIKE], ascending=True)
    if len(call_filter) == 0:
        return None
    session_coeff = math.pow(1.0 - autoassets.schedule.get_session_coeff(), 10.0)
    call_cumoi = call_filter[[autoassets.OptionContractField.OPEN_INTEREST, autoassets.OptionContractField.VOLUME]].cumsum()
    call_cumoi['oi'] = session_coeff * call_cumoi[autoassets.OptionContractField.OPEN_INTEREST] + call_cumoi[autoassets.OptionContractField.VOLUME]
    # Return first contract with minimum required open interest.
    min_oi = (session_coeff * call_filter[autoassets.OptionContractField.OPEN_INTEREST] + call_filter[autoassets.OptionContractField.VOLUME]).sum() * MIN_PIVOT_INTEREST_FRACTION
    call_cumoi_filter = call_cumoi[call_cumoi['oi'] >= min_oi]
    if len(call_cumoi_filter) == 0:
        logger.debug('Insufficient open interest in calls.')
        return None
    cpivot_cumoi = call_cumoi_filter.iloc[0]
    cpivot = option_chain.loc[cpivot_cumoi.name]
    logger.debug('Call-side pivot = {} (delta = {}).'.format(cpivot[autoassets.OptionContractField.STRIKE], cpivot[autoassets.OptionContractField.DELTA]))
    return cpivot
#END: get_cpivot

def get_ppivot(option_chain, oi=False, volume=True):
    """
    Return put-side pivot, if any.

    Parameters
    ----------
    option_chain: pd.DataFrame
        Option chain.

    oi: bool (default: False)
        Factor in open interest.

    volume: bool (default: True)
        Factor in volume.

    Returns
    -------
    pd.Series:
        Put-side pivot contract.
    """
    # Calculate cumulative sum of open interest and volume per strike on the put side.
    put_filter = option_chain[
        (option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.PUT) &
        (option_chain[autoassets.OptionContractField.DELTA] <= -0.10) &
        (option_chain[autoassets.OptionContractField.DELTA] >= -0.90)
        ].sort_values(by=[autoassets.OptionContractField.STRIKE], ascending=False)
    if len(put_filter) == 0:
        return None
    #session_coeff = math.pow(1.0 - autoassets.schedule.get_session_coeff(), 10.0)
    put_cumoi = put_filter[[autoassets.OptionContractField.OPEN_INTEREST, autoassets.OptionContractField.VOLUME]].cumsum()
    #put_cumoi['oi'] = session_coeff * put_cumoi[autoassets.OptionContractField.OPEN_INTEREST] + put_cumoi[autoassets.OptionContractField.VOLUME]
    put_cumoi['oi'] = (1.0 if oi else 0.0) * put_cumoi[autoassets.OptionContractField.OPEN_INTEREST] + (1.0 if volume else 0.0) * put_cumoi[autoassets.OptionContractField.VOLUME]
    # Return first contract with minimum required open interest.
    #min_oi = (session_coeff * put_filter[autoassets.OptionContractField.OPEN_INTEREST] + put_filter[autoassets.OptionContractField.VOLUME]).sum() * MIN_PIVOT_INTEREST_FRACTION
    min_oi = 300000
    put_cumoi_filter = put_cumoi[put_cumoi['oi'] >= min_oi]
    if len(put_cumoi_filter) == 0:
        logger.debug('Insufficient open interest in puts.')
        return None
    ppivot_cumoi = put_cumoi_filter.iloc[0]
    ppivot = option_chain.loc[ppivot_cumoi.name]
    logger.debug('Put-side pivot = {} (delta = {}).'.format(ppivot[autoassets.OptionContractField.STRIKE], ppivot[autoassets.OptionContractField.DELTA]))
    return ppivot
#END: get_ppivot

def get_lpivot(option_chain):
    """
    Return lower pivot, if any.

    Parameters
    ----------
    option_chain: pd.DataFrame
        Option chain.

    Returns
    -------
    pd.Series:
        Lower pivot contract.
    """
    # Calculate cumulative sum of open interest and volume per strike on the put side.
    lower_filter = option_chain.sort_values(by=[autoassets.OptionContractField.STRIKE], ascending=False)
    if len(lower_filter) == 0:
        return None
    session_coeff = 1.0 #math.pow(1.0 - autoassets.schedule.get_session_coeff(), 10.0)
    lower_cumoi = lower_filter[[autoassets.OptionContractField.OPEN_INTEREST, autoassets.OptionContractField.VOLUME]].cumsum()
    lower_cumoi['oi'] = session_coeff * lower_cumoi[autoassets.OptionContractField.OPEN_INTEREST] + lower_cumoi[autoassets.OptionContractField.VOLUME]
    # Return first contract with minimum required open interest.
    min_oi = (session_coeff * lower_filter[autoassets.OptionContractField.OPEN_INTEREST] + lower_filter[autoassets.OptionContractField.VOLUME]).sum() * MIN_PIVOT_INTEREST_FRACTION
    lower_cumoi_filter = lower_cumoi[lower_cumoi['oi'] >= min_oi]
    if len(lower_cumoi_filter) == 0:
        logger.debug('Insufficient open interest.')
        return None
    lpivot_cumoi = lower_cumoi_filter.iloc[0]
    lpivot = option_chain.loc[lpivot_cumoi.name]
    logger.debug('Lower pivot = {} (delta = {}).'.format(lpivot[autoassets.OptionContractField.STRIKE], lpivot[autoassets.OptionContractField.DELTA]))
    return lpivot
#END: get_lpivot

def get_upivot(option_chain):
    """
    Return upper pivot.

    Parameters
    ----------
    option_chain: pd.DataFrame
        Option chain.

    Returns
    -------
    pd.Series:
        Upper pivot contract.
    """
    # Calculate cumulative sum of open interest and volume per strike on the call side.
    upper_filter = option_chain.sort_values(by=[autoassets.OptionContractField.STRIKE], ascending=True)
    if len(upper_filter) == 0:
        return None
    session_coeff = 1.0 #math.pow(1.0 - autoassets.schedule.get_session_coeff(), 10.0)
    upper_cumoi = upper_filter[[autoassets.OptionContractField.OPEN_INTEREST, autoassets.OptionContractField.VOLUME]].cumsum()
    upper_cumoi['oi'] = session_coeff * upper_cumoi[autoassets.OptionContractField.OPEN_INTEREST] + upper_cumoi[autoassets.OptionContractField.VOLUME]
    # Return first contract with minimum required open interest.
    min_oi = (session_coeff * upper_filter[autoassets.OptionContractField.OPEN_INTEREST] + upper_filter[autoassets.OptionContractField.VOLUME]).sum() * MIN_PIVOT_INTEREST_FRACTION
    upper_cumoi_filter = upper_cumoi[upper_cumoi['oi'] >= min_oi]
    if len(upper_cumoi_filter) == 0:
        logger.debug('Insufficient open interest.')
        return None
    upivot_cumoi = upper_cumoi_filter.iloc[0]
    upivot = option_chain.loc[upivot_cumoi.name]
    logger.debug('Upper pivot = {} (delta = {}).'.format(upivot[autoassets.OptionContractField.STRIKE], upivot[autoassets.OptionContractField.DELTA]))
    return upivot
#END: get_upivot

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
    positioning = asset['definition']['positioning']
    structure = positioning['structure']
    return structure.neutralize(asset, quote_db, option_chain_db, backend_setting)
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
    structure = positioning['structure']
    #trades_available = structure.availability(asset, quote_db, option_chain_db)['bearish_trades']
    #if trades_available == 0:
    #    logger.debug('Abort trade: Zero bearish trades available.')
    #    return False
    if not structure.place_bearish_trade(asset, quote_db, option_chain_db, backend_setting):
        return False
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
    structure = positioning['structure']
    #trades_available = structure.availability(asset, quote_db, option_chain_db)['bullish_trades']
    #if trades_available == 0:
    #    logger.debug('Abort trade: Zero bullish trades available.')
    #    return False
    if not structure.place_bullish_trade(asset, quote_db, option_chain_db, backend_setting):
        return False
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
    positioning = asset['definition']['positioning']
    structure = positioning['structure']
    return structure.primary_quote(positioning, quote_db, option_chain_db)
#END: primary_quote
