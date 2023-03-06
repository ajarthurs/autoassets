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
Asset management.
"""

import autoassets.positioning
import autoassets.schedule
import autoassets.strategy
import copy
import json
import logging
import pandas as pd
from enum import Enum, auto
from functools import reduce

logger = logging.getLogger(__name__)

class BudgetUnit(Enum):
    """
    Budget unit.
    """
    DOLLAR = 'dollar'
    SHARE  = 'share'

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_value(cls, name)
#END: BudgetUnit

class ChartBarField(Enum):
    """
    Instrument bar field.
    """
    TIMESTAMP         = auto()
    OPEN_PRICE        = auto()
    HIGH_PRICE        = auto()
    LOW_PRICE         = auto()
    CLOSE_PRICE       = auto()
    VOLUME            = auto()
    CUMULATIVE_VOLUME = auto() # For by-volume frames only.
#END: ChartBarField

class InstrumentFrame(Enum):
    """
    Framing of instrument.
    """
    MINUTELY           = auto()
    QUARTER_HOURLY     = auto()
    HALF_HOURLY        = auto()
    HOURLY             = auto()
    DAILY              = auto()
    WEEKLY             = auto()
    MONTHLY            = auto()
    BY_QUOTE           = auto()
    BY_TICK            = auto()
    BY_INTRADAY_VOLUME = auto()
    BY_DAILY_VOLUME    = auto()
#END: InstrumentFrame

class OrderDirection(Enum):
    """
    Order direction.
    """
    BUY           = auto()
    SELL          = auto()
    BUY_TO_OPEN   = auto()
    SELL_TO_OPEN  = auto()
    BUY_TO_CLOSE  = auto()
    SELL_TO_CLOSE = auto()
#END: OrderDirection

class OptionContractField(Enum):
    """
    Option contract field.
    """
    # Raw fields
    SYMBOL                = auto()
    DESCRIPTION           = auto()
    OPEX                  = auto()
    STRIKE                = auto()
    CONTRACT_TYPE         = auto()
    BID_PRICE             = auto()
    ASK_PRICE             = auto()
    LAST_PRICE            = auto()
    MARK_PRICE            = auto()
    DELTA                 = auto()
    GAMMA                 = auto()
    THETA                 = auto()
    VOLATILITY            = auto()
    OPEN_INTEREST         = auto()
    VOLUME                = auto()
    # Generated fields
    EXTRINSIC_MARK_PRICE  = auto()
    EXTRINSIC_BID_PRICE   = auto()
    EXTRINSIC_ASK_PRICE   = auto()
    PREV_VOLUME           = auto()
    VOLUME_BIAS           = auto()
#END: OptionContractField

class OptionContractType(Enum):
    """
    Option contract type.
    """
    CALL = 'CALL'
    PUT  = 'PUT'
    ALL  = '*'
    UNSUPPORTED = ''

    @classmethod
    def _missing_(cls, name):
        return _enum_case_insensitive_search_by_value(cls, name)
#END: OptionContractType

class QuoteField(Enum):
    """
    Instrument quote field.
    """
    DESCRIPTION = auto()
    BID_PRICE   = auto()
    ASK_PRICE   = auto()
    LAST_PRICE  = auto()
    MARK_PRICE  = auto()
    # Generated fields
    PREV_MARK_PRICE = auto()
#END: QuoteField

def active_backends(assets):
    """
    Return a unique list of backends referenced by active assets.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    Returns
    -------
    [str]:
        Unique list of active backends.
    """
    backends = []
    for asset in assets:
        if not autoassets.asset_active(asset):
            continue
        positioning_backend = autoassets.positioning.backend(asset['definition']['positioning'])
        if positioning_backend not in backends:
            backends.append(positioning_backend)
        strategy_backends = autoassets.strategy.backends(asset['definition']['strategy'])
        for backend in strategy_backends:
            if backend in backends:
                continue
            backends.append(backend)
    return backends
#END: active_backends

def asset_active(asset):
    """
    Whether or not this asset is active.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    bool:
        True if active; False otherwise.
    """
    return 'enable' in asset and asset['enable']
#END: asset_active

def budget(asset):
    """
    Return the asset's long and short budgets.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    Returns
    -------
    (short_budget, long_budget):
        Short and long budgets in total dollar amount, floating-point.
    """
    if 'budget' not in asset:
        logger.error('Undefined budget on asset: {}'.format(asset))
        return None
    budget = asset['budget']
    if type(budget) is dict: # Supplemental information in budget (typically unit type).
        if 'unit' not in budget:
            logger.error('Missing "unit" in budget on asset: {}'.format(asset))
            return None
        unit = autoassets.BudgetUnit(budget['unit'])
        budget = budget['amount']
    else: # Assume dollar unit.
        unit = autoassets.BudgetUnit.DOLLAR
    if type(budget) is list: # Bidirectional asset.
        if len(budget) != 2:
            logger.error('Illegal budget length on asset: {}'.format(asset))
            return None
        budget_float = [float(side) for side in budget]
        budget_short = min(budget_float)
        budget_long = max(budget_float)
        if budget_short > 0 or budget_long < 0 or budget_short >= budget_long:
            logger.error('Illegal budget range on asset: {}'.format(asset))
            return None
    elif float(budget) > 0: # Long-only asset.
        budget_long = float(budget)
        budget_short = 0.0
    elif float(budget) < 0: # Short-only asset.
        budget_long = 0.0
        budget_short = float(budget)
    else: # zero budget
        budget_long = 0.0
        budget_short = 0.0
        #logger.error('Zero budget on asset: {}'.format(asset))
        #return None
    return {
        'unit': unit,
        'amount': (budget_short, budget_long),
    }
#END: budget

def connect_to_streams(backends_setting, assets):
    """
    Setup connections for all backends referenced by active assets.

    Parameters
    ----------
    backends_setting: dict
        Per-backend settings.

    assets: [dict]
        List of assets.

    Returns
    -------
    [dict]:
        List of streams.
    """
    backends = active_backends(assets)
    streams = []
    for backend in backends:
        setting = backends_setting[backend]
        streams.append({
            'backend': backend,
            'socket': backend.connect_to_stream(setting),
        })
    return streams
#END: connect_to_streams

def disconnect_from_streams(streams):
    """
    Setup connections for all backends referenced by active assets.

    Parameters
    ----------
    streams: [dict]
        List of streams.
    """
    for stream in streams:
        stream['backend'].disconnect_from_stream(stream['socket'])
#END: disconnect_from_streams

def fetch_historical_data(assets):
    """
    Populate instrument database for the given assets.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    Returns
    -------
    dict:
        Instrument database.
    """
    instrument_db = {}
    instruments = autoassets.strategy.active_instruments(assets)
    for instrument in instruments:
        frame = instrument['frame']
        ticker = instrument['ticker']
        if frame not in instrument_db:
            instrument_db[frame] = {}
        instrument_db[frame][ticker] = instrument['backend'].fetch_historical_data(instrument)
    return instrument_db
#END: fetch_historical_data

def fetch_option_chains(assets, quote_db):
    """
    Populate option chain database for the given assets.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    quote_db: dict
        Quote database.

    Returns
    -------
    dict:
        Option chain database.
    """
    option_chain_db = {}
    instruments = autoassets.positioning.active_instruments(assets, require_optionable=True)
    if len(instruments) == 0:
        return option_chain_db
    instrument_df = pd.DataFrame(instruments)
    backend_gb = instrument_df.groupby('backend')
    for backend, instruments in backend_gb:
        if len(instruments) == 0:
            continue
        tickers = list(instruments['ticker'])
        ref_instrument = instruments.iloc[0]
        contract_type = OptionContractType.ALL if 'contract_type' not in ref_instrument else ref_instrument['contract_type']
        max_dte = None if 'max_dte' not in ref_instrument else int(ref_instrument['max_dte'])
        strike_count = 0 if 'strike_count' not in ref_instrument else int(ref_instrument['strike_count'])
        option_chain_db.update(backend.fetch_option_chains(tickers, quote_db, contract_type=contract_type, max_dte=max_dte, strike_count=strike_count))
    return option_chain_db
#END: fetch_option_chains

def fetch_quotes(assets):
    """
    Populate quote database for the given assets.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    Returns
    -------
    dict:
        Quote database.
    """
    quote_db = {}
    instrument_df = pd.DataFrame(autoassets.positioning.active_instruments(assets))
    backend_gb = instrument_df.groupby('backend')
    for backend, instruments in backend_gb:
        tickers = list(instruments['ticker'])
        quote_db.update(backend.fetch_quotes(tickers))
    return quote_db
#END: fetch_quotes

def get_tickers_from_instruments(instruments):
    """
    Return a unique list of tickers from a list of instruments.

    Parameters
    ----------
    instruments: [dict]
        List of instruments.

    Returns
    -------
    [str]:
        Unique list of tickers.
    """
    return list(set([instrument['ticker'] for instrument in instruments]))
#END: get_tickers_from_instruments

def get_tickers_from_option_chains(option_chain_db, instruments):
    """
    Return a unique list of option contract tickers with respect to a list of instruments.

    Parameters
    ----------
    option_chain_db: dict
        Option chain database.

    instruments: [dict]
        List of instruments.

    Returns
    -------
    {ticker: [str]}:
        Unique list of option contract tickers, keyed by underlying ticker.
    """
    tickers = get_tickers_from_instruments(instruments)
    return {ticker: [contract_ticker
        for contract_ticker in option_chain_db[ticker][OptionContractField.SYMBOL]
        ]
        for ticker in tickers
    }
#END: get_tickers_from_option_chains

def listen_to_streams(streams, flags):
    """
    Listen for and process messages.

    Parameters
    ----------
    streams: [dict]
        List of streams to listen to.

    flags: dict
        A dictionary that contains the following:
            'done': Set to True to terminate the listeners.
    """
    #XXX: Consider asyncio to run stream listeners in parallel. This is currently sequential.
    for stream in streams:
        stream['backend'].listen_to_stream(
            stream['socket'],
            flags,
        )
#END: listen_to_streams

def load_assets(assets_definition, assets_json_path):
    """
    Build list of assets from blueprint and JSON file.

    Parameters
    ----------
    assets_definition: dict
        Blueprint of assets.

    assets_json_path: str
        Path to JSON file.

    Returns
    -------
    [dict]:
        List of asset specifications. None if error.
    """
    assets = []
    try:
        with open(assets_json_path, 'r') as f:
            assets = json.load(f)
    except:
        logger.exception('Failed to load assets: {}'.format(assets_json_path))
        return None
    for asset in assets:
        klass = asset['class']
        if klass not in assets_definition:
            logger.error('Missing asset class: {}'.format(klass))
            return None
        asset['definition'] = _resolve_references(asset, assets_definition[klass])
    return assets
#END: load_assets

def store_assets(assets, assets_json_path):
    """
    Store assets into JSON file.

    Parameters
    ----------
    assets: [dict]
        List of assets.

    assets_json_path: str
        Path to JSON file.
    """
    assets_copy = []
    for asset in assets:
        asset_copy = {}
        for key,value in asset.items():
            if key == 'definition':
                continue
            asset_copy[key] = copy.deepcopy(value)
        assets_copy.append(asset_copy)
    try:
        with open(assets_json_path, 'w') as f:
            json.dump(assets_copy, f, indent=4)
    except:
        logger.exception('Failed to store assets: {}'.format(assets_json_path))
#END: store_assets

def subscribe_to_historical_data(streams, assets, instrument_db, assets_json_path):
    """
    Link instrument database (dict) to the given streams.

    Parameters
    ----------
    streams: [dict]
        List of streams.

    assets: [dict]
        List of assets.

    instrument_db: dict
        Instrument database.

    assets_json_path: str
        Path to assets JSON file.
    """
    instruments = autoassets.strategy.active_instruments(assets)
    for stream in streams:
        backend_instruments = [instrument for instrument in instruments if instrument['backend'] == stream['backend']]
        stream['backend'].subscribe_to_historical_data(
            stream['socket'],
            instrument_db,
            backend_instruments,
            cb_functions = [
                _cb_store_assets,
            ],
            cb_data = [
                {
                    'assets': assets,
                    'assets_json_path': assets_json_path,
                },
            ],
        )
#END: subscribe_to_historical_data

def subscribe_to_option_data(streams, assets, option_chain_db, quote_db):
    """
    Link option chain database (dict) to the given streams.

    Parameters
    ----------
    streams: [dict]
        List of streams.

    assets: [dict]
        List of assets.

    option_chain_db: dict
        Option chain database.

    quote_db: dict
        Quote database.
    """
    instruments = autoassets.positioning.active_instruments(assets, require_optionable=True)
    for stream in streams:
        backend_instruments = [instrument for instrument in instruments if instrument['backend'] == stream['backend']]
        stream['backend'].subscribe_to_option_data(
            stream['socket'],
            option_chain_db,
            backend_instruments,
            quote_db,
        )
#END: subscribe_to_option_data

def subscribe_to_quotes(streams, assets, instrument_db, option_chain_db, quote_db, backends_setting):
    """
    Link quote database (dict) to the given streams, and register the trading strategy callbacks.

    Parameters
    ----------
    streams: [dict]
        List of streams.

    assets: [dict]
        List of assets.

    instrument_db: dict
        Instrument database.

    option_chain_db: dict
        Option chain database.

    quote_db: dict
        Quote database.

    backends_setting: dict
        Per-backend settings.
    """
    instruments = autoassets.positioning.active_instruments(assets)
    for stream in streams:
        backend_instruments = [instrument for instrument in instruments if instrument['backend'] == stream['backend']]
        stream['backend'].subscribe_to_quotes(
            stream['socket'],
            quote_db,
            backend_instruments,
            cb_functions = [
                autoassets.strategy.cb_run_strategies,
                autoassets.positioning.cb_run_probes,
            ],
            cb_data = [
                {
                    'assets': assets,
                    'instrument_db': instrument_db,
                    'option_chain_db': option_chain_db,
                    'quote_db': quote_db,
                    'backend_setting': backends_setting[stream['backend']],
                },
                {
                    'assets': assets,
                    'instrument_db': instrument_db,
                    'option_chain_db': option_chain_db,
                    'quote_db': quote_db,
                    'backend_setting': backends_setting[stream['backend']],
                },
            ],
        )
#END: subscribe_to_quotes

#################
# LOW-LEVEL API #
#################

def _cb_store_assets(unused, store_assets_params):
    """
    Callback to store assets into JSON file.

    Parameters
    ----------
    unused: any
        Reserved.

    store_assets_params: dict
        Parameters for store assets
    """
    if not autoassets.schedule.is_now_normal_market_hours():
        return
    store_assets(store_assets_params['assets'], store_assets_params['assets_json_path'])
#END: _cb_store_assets

def _enum_case_insensitive_search_by_value(cls, name):
    """
    Return first member whose value matches the given name.

    Parameters
    ----------
    cls: Enum
        Enumeration subclass.

    name: str
        String to search for.

    Returns
    -------
    Enum:
        Matching enumeration member or None.
    """
    matches = [member for member in cls if member.value.lower() == name.strip().lower()]
    #for member in cls:
    #    if member.value.lower() == name.strip().lower():
    #        return member
    if len(matches) > 0:
        return matches[0]
    else:
        return cls.UNSUPPORTED
#END: _enum_case_insensitive_search_by_value

def _resolve_references(asset, unresolved):
    """
    Resolve all references in dictionary.

    Parameters
    ----------
    asset: dict
        Asset specifications.

    unresolved: dict
        Unresolved dictionary.

    Returns
    -------
    dict:
        Resolved dictionary.
    """
    resolved = {}
    for key,unresolved_item in unresolved.items():
        if type(unresolved_item) is dict and 'ref' in unresolved_item:
            resolved_item = reduce(dict.get, unresolved_item['ref'].split("."), asset)
            if resolved_item is None and 'default' in unresolved_item:
                resolved_item = unresolved_item['default']
            elif resolved_item is None:
                logger.error('Failed to resolve item: {}'.format(unresolved_item))
                continue
            resolved[key] = resolved_item
        elif type(unresolved_item) is dict:
            resolved[key] = _resolve_references(asset, unresolved_item)
        else:
            resolved[key] = unresolved_item
    return resolved
#END: _resolve_references
