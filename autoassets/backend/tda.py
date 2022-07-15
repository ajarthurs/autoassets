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
TDA API/Websockets glue logic.
"""

import autoassets
import autoassets.schedule
from datetime import date, timedelta
import logging
import math
import numpy as np
import pandas as pd
import sys
import tda
import tda.api
import tda.streaming
import time

logger = logging.getLogger(__name__)

# Bind classes/enumerations.
InstrumentType = tda.InstrumentType
WSQOSLevel = tda.streaming.WSQOSLevel

def connect_to_stream(setting):
    """
    Setup connection to TDA's WebSockets server.

    Parameters
    ----------
    setting: dict
        TDA settings.

    Returns
    -------
    dict:
        Stream specifications.
    """
    return tda.streaming.ws_connect(setting['account_id'], setting['qos'])
#END: connect_to_stream

def disconnect_from_stream(stream):
    """
    Disconnect from TDA's WebSockets server.

    Parameters
    ----------
    stream: dict
        Stream specifications.
    """
    tda.streaming.ws_disconnect(stream)
#END: disconnect_from_stream

def fetch_historical_data(instrument):
    """
    Fetch historical data for instrument.

    Parameters
    ----------
    instrument: dict
        Instrument specifications.

    Returns
    -------
    pd.DataFrame:
        Historical data for instrument. None if error.
    """
    frame = instrument['frame']
    if frame == autoassets.InstrumentFrame.BY_TICK:
        #XXX: Check the TDA API for historical tick (time&sales) data. Currently does not exist, so return an initially empty dataframe that will be populated later by the time&sales streaming service. May also look for a source outside of TDA.
        df = pd.DataFrame()
        df.index.rename(autoassets.ChartBarField.TICK, inplace=True)
        return df
    period_type = None
    period = None
    frequency_type = None
    frequency = None
    if frame == autoassets.InstrumentFrame.MINUTELY or frame == autoassets.InstrumentFrame.BY_INTRADAY_VOLUME:
        period_type = tda.api.PeriodType.DAY
        period = 10
        frequency_type = tda.api.FrequencyType.MINUTELY
        frequency = 1
    elif frame == autoassets.InstrumentFrame.QUARTER_HOURLY:
        period_type = tda.api.PeriodType.DAY
        period = 10
        frequency_type = tda.api.FrequencyType.MINUTELY
        frequency = 15
    elif(
        frame == autoassets.InstrumentFrame.HALF_HOURLY or
        frame == autoassets.InstrumentFrame.HOURLY
    ):
        period_type = tda.api.PeriodType.DAY
        period = 10
        frequency_type = tda.api.FrequencyType.MINUTELY
        frequency = 30
    elif frame == autoassets.InstrumentFrame.DAILY or frame == autoassets.InstrumentFrame.BY_DAILY_VOLUME:
        period_type = tda.api.PeriodType.YEAR
        period = 20
        frequency_type = tda.api.FrequencyType.DAILY
        frequency = 1
    elif frame == autoassets.InstrumentFrame.WEEKLY:
        period_type = tda.api.PeriodType.YEAR
        period = 20
        frequency_type = tda.api.FrequencyType.WEEKLY
        frequency = 1
    elif frame == autoassets.InstrumentFrame.MONTHLY:
        period_type = tda.api.PeriodType.YEAR
        period = 20
        frequency_type = tda.api.FrequencyType.MONTHLY
        frequency = 1
    else:
        logger.error('Unsupported instrument frame: {}'.format(frame))
        return None
    raw_data = None
    while raw_data is None:
        raw_data = tda.api.get_history(
            instrument['ticker'],
            period_type,
            period,
            frequency_type,
            frequency,
        )
        if raw_data is None:
            logger.warn('Failed to fetch data for instrument: {}'.format(instrument))
            time.sleep(1)
    raw_df = pd.DataFrame(raw_data)
    df = None
    if(
        frame == autoassets.InstrumentFrame.MINUTELY or
        frame == autoassets.InstrumentFrame.QUARTER_HOURLY or
        frame == autoassets.InstrumentFrame.HALF_HOURLY or
        frame == autoassets.InstrumentFrame.HOURLY or
        frame == autoassets.InstrumentFrame.DAILY or
        frame == autoassets.InstrumentFrame.WEEKLY or
        frame == autoassets.InstrumentFrame.MONTHLY
    ):
        raw_df.index = pd.to_datetime(raw_df[tda.ChartBarField.TIMESTAMP], unit='ms', utc=True)
        if(
            frame != autoassets.InstrumentFrame.MINUTELY and
            frame != autoassets.InstrumentFrame.QUARTER_HOURLY and
            frame != autoassets.InstrumentFrame.HALF_HOURLY
        ):
            if frame == autoassets.InstrumentFrame.HOURLY:
                a = raw_df[raw_df.index.minute == 0]
                b = raw_df[raw_df.index.minute == 30]
                b.index = b.index.map(lambda timestamp: timestamp.replace(minute=0, second=0))
                high_df = pd.DataFrame({'a':a[tda.ChartBarField.HIGH_PRICE], 'b':b[tda.ChartBarField.HIGH_PRICE]})
                low_df = pd.DataFrame({'a':a[tda.ChartBarField.LOW_PRICE], 'b':b[tda.ChartBarField.LOW_PRICE]})
                raw_df = pd.DataFrame()
                raw_df[tda.ChartBarField.OPEN_PRICE]  = a[tda.ChartBarField.OPEN_PRICE]
                raw_df[tda.ChartBarField.HIGH_PRICE]  = high_df.max(axis=1)
                raw_df[tda.ChartBarField.LOW_PRICE]   = low_df.min(axis=1)
                raw_df[tda.ChartBarField.CLOSE_PRICE] = b[tda.ChartBarField.CLOSE_PRICE]
                raw_df[tda.ChartBarField.VOLUME]      = a[tda.ChartBarField.VOLUME] + b[tda.ChartBarField.VOLUME]
            else:
                raw_df.index = raw_df.index.normalize()
        df = pd.DataFrame(index=raw_df.index)
        df.index.rename(autoassets.ChartBarField.TIMESTAMP, inplace=True)
        df[autoassets.ChartBarField.OPEN_PRICE]  = raw_df[tda.ChartBarField.OPEN_PRICE]
        df[autoassets.ChartBarField.HIGH_PRICE]  = raw_df[tda.ChartBarField.HIGH_PRICE]
        df[autoassets.ChartBarField.LOW_PRICE]   = raw_df[tda.ChartBarField.LOW_PRICE]
        df[autoassets.ChartBarField.CLOSE_PRICE] = raw_df[tda.ChartBarField.CLOSE_PRICE]
        df[autoassets.ChartBarField.VOLUME]      = raw_df[tda.ChartBarField.VOLUME]
    elif(
        frame == autoassets.InstrumentFrame.BY_INTRADAY_VOLUME or
        frame == autoassets.InstrumentFrame.BY_DAILY_VOLUME
    ):
        #XXX: Too slow converting time- to volume-series. Takes on the order of 10 seconds.
        #volume_interval = int(raw_df[tda.ChartBarField.VOLUME].max()) # Pick max-volume to reduce discrete series conversion error.
        logger.debug('instrument={}'.format(instrument))
        volume_interval = instrument['volume_interval'] if 'volume_interval' in instrument else  int(raw_df[tda.ChartBarField.VOLUME].mean() + 3.0 * raw_df[tda.ChartBarField.VOLUME].std()) # Pick volume that covers 3 standard deviations of data.
        total_volume = int(raw_df[tda.ChartBarField.VOLUME].sum())
        total_div_volume = int(total_volume / volume_interval) * volume_interval
        total_time_bars = len(raw_df.index)
        time_bar_idx = total_time_bars - 1
        df = pd.DataFrame()
        df.index.rename(autoassets.ChartBarField.CUMULATIVE_VOLUME, inplace=True)
        volume_idx = 0
        while volume_idx < total_div_volume:
            time_bars = []
            volume = 0
            if time_bar_idx < 0:
                break
            time_bar = raw_df.iloc[time_bar_idx]
            if time_bar[tda.ChartBarField.VOLUME] > volume_interval: # Split outlier time-bar.
                num_split_bars = int(time_bar[tda.ChartBarField.VOLUME] / volume_interval)
                remaining_volume = time_bar[tda.ChartBarField.VOLUME] % volume_interval
                num_remaining_bars = 1 if remaining_volume > 0 else 0
                split_price_slope = (time_bar[tda.ChartBarField.CLOSE_PRICE] - time_bar[tda.ChartBarField.OPEN_PRICE]) / (num_split_bars + num_remaining_bars)
                split_price = time_bar[tda.ChartBarField.OPEN_PRICE]
                for split_bar_idx in range(num_split_bars):
                    df.loc[volume_idx, autoassets.ChartBarField.OPEN_PRICE] = split_price
                    df.loc[volume_idx, autoassets.ChartBarField.HIGH_PRICE] = time_bar[tda.ChartBarField.HIGH_PRICE]
                    df.loc[volume_idx, autoassets.ChartBarField.LOW_PRICE]  =  time_bar[tda.ChartBarField.LOW_PRICE]
                    df.loc[volume_idx, autoassets.ChartBarField.VOLUME]     = volume_interval
                    df.loc[volume_idx, autoassets.ChartBarField.TIMESTAMP]  = pd.to_datetime(time_bar[tda.ChartBarField.TIMESTAMP], unit='ms', utc=True)
                    split_price = split_price_slope * (split_bar_idx + 1) + time_bar[tda.ChartBarField.OPEN_PRICE]
                    df.loc[volume_idx, autoassets.ChartBarField.CLOSE_PRICE] = split_price
                    volume_idx += volume_interval
                if remaining_volume > 0: # Defer remaining bar for merging.
                    time_bar = {
                        tda.ChartBarField.OPEN_PRICE  : split_price,
                        tda.ChartBarField.HIGH_PRICE  : time_bar[tda.ChartBarField.HIGH_PRICE],
                        tda.ChartBarField.LOW_PRICE   : time_bar[tda.ChartBarField.LOW_PRICE],
                        tda.ChartBarField.CLOSE_PRICE : time_bar[tda.ChartBarField.CLOSE_PRICE],
                        tda.ChartBarField.VOLUME      : remaining_volume,
                        tda.ChartBarField.TIMESTAMP   : time_bar[tda.ChartBarField.TIMESTAMP],
                    }
                else: # No remaining bar. Iterate to next time-bar.
                    time_bar_idx -= 1
                    volume_idx += volume_interval
                    continue
            # Merge time-bars by approximately volume_interval.
            while (time_bar_idx >= 0) and (abs(volume_interval - (volume + time_bar[tda.ChartBarField.VOLUME])) <= abs(volume_interval - volume)):
                volume += time_bar[tda.ChartBarField.VOLUME]
                time_bars.append(time_bar)
                time_bar_idx -= 1
                if time_bar_idx >= 0:
                    time_bar = raw_df.iloc[time_bar_idx]
            if len(time_bars) == 0: #XXX: Shouldn't happen.
                logger.error('BUG: aggregating no time-bars for the current volume-bar.')
            df.loc[volume_idx, autoassets.ChartBarField.OPEN_PRICE]  = time_bars[0][tda.ChartBarField.OPEN_PRICE]
            df.loc[volume_idx, autoassets.ChartBarField.HIGH_PRICE]  = np.max([time_bar[tda.ChartBarField.HIGH_PRICE] for time_bar in time_bars])
            df.loc[volume_idx, autoassets.ChartBarField.LOW_PRICE]   = np.min([time_bar[tda.ChartBarField.LOW_PRICE] for time_bar in time_bars])
            df.loc[volume_idx, autoassets.ChartBarField.CLOSE_PRICE] = time_bars[-1][tda.ChartBarField.CLOSE_PRICE]
            df.loc[volume_idx, autoassets.ChartBarField.VOLUME]      = volume
            df.loc[volume_idx, autoassets.ChartBarField.TIMESTAMP]   = pd.to_datetime(time_bars[0][tda.ChartBarField.TIMESTAMP], unit='ms', utc = True)
            volume_idx += volume_interval
        df.sort_index(ascending=False, inplace=True)
        #import matplotlib.pyplot as plt
        #df[[autoassets.ChartBarField.OPEN_PRICE, autoassets.ChartBarField.HIGH_PRICE, autoassets.ChartBarField.LOW_PRICE, autoassets.ChartBarField.CLOSE_PRICE]].plot()
        #plt.show()
    return df
#END: fetch_historical_data

def fetch_option_chains(tickers, quote_db, min_dte=None, max_dte=None, strike_count=0):
    """
    Fetch option chains for tickers.

    Parameters
    ----------
    tickers: [str]
        List of tickers.

    quote_db: dict
        Quote database.

    min_dte: int (default: earliest available)
        Minimum days till expiration.

    max_dte: int (default: latest available)
        Maximum days till expiration.

    strike_count: int (default: all strikes)
        Number of strikes about the ATM strike.

    Returns
    -------
    dict:
        Option chain data keyed by symbol.
    """
    from_date = None if min_dte is None else (date.today()+timedelta(days=min_dte))
    to_date = None if max_dte is None else (date.today()+timedelta(days=max_dte))
    raw_option_chain_dict = tda.api.get_option_chains(
        tickers,
        from_date=from_date, to_date=to_date,
        strike_count=strike_count,
        )
    option_chain_dict = {ticker: map_option_chain_dict(raw_option_chain)
        for ticker, raw_option_chain in raw_option_chain_dict.items()
    }
    for ticker, option_chain in option_chain_dict.items():
        quote = quote_db[ticker]
        if autoassets.QuoteField.MARK_PRICE not in quote: #XXX: Field population hack for indexes such as $SPX.X.
            quote[autoassets.QuoteField.MARK_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
            quote[autoassets.QuoteField.BID_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
            quote[autoassets.QuoteField.ASK_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
        quote[autoassets.QuoteField.PREV_MARK_PRICE] = quote[autoassets.QuoteField.MARK_PRICE]
        option_chain[autoassets.OptionContractField.VOLUME] = 0
        option_chain[autoassets.OptionContractField.PREV_VOLUME] = 0
        option_chain[autoassets.OptionContractField.VOLUME_BIAS] = 0
        sign = np.where(option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.CALL, 1.0, -1.0)
        option_chain[autoassets.OptionContractField.EXTRINSIC_MARK_PRICE] = (option_chain[autoassets.OptionContractField.MARK_PRICE] - (sign * (quote[autoassets.QuoteField.MARK_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
        option_chain[autoassets.OptionContractField.EXTRINSIC_BID_PRICE] = (option_chain[autoassets.OptionContractField.BID_PRICE] - (sign * (quote[autoassets.QuoteField.BID_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
        option_chain[autoassets.OptionContractField.EXTRINSIC_ASK_PRICE] = (option_chain[autoassets.OptionContractField.ASK_PRICE] - (sign * (quote[autoassets.QuoteField.ASK_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
    return option_chain_dict
#END: fetch_option_chains

def fetch_quotes(tickers):
    """
    Fetch quotes for tickers.

    Parameters
    ----------
    tickers: [str]
        List of tickers.

    Returns
    -------
    dict:
        Quote data keyed by symbol.
    """
    raw_quote_dict = tda.api.get_quotes(tickers)
    quote_dict = {ticker: map_quote_dict(raw_quote)
        for ticker, raw_quote in raw_quote_dict.items()
    }
    return quote_dict
#END: fetch_quotes

def listen_to_stream(socket, flags):
    tda.streaming.ws_listen(socket, flags)
#END: listen_to_stream

def map_option_chain_dict(raw_option_chain):
    """
    Map TDA's option chain to AutoAssets option chain.

    Parameters
    ----------
    raw_option_chain: dict
        TDA's option chain.

    Returns
    -------
    dict:
        AutoAssets option chain.
    """
    df = pd.DataFrame(columns=[autoassets.OptionContractField.SYMBOL])
    df.index.rename(autoassets.OptionContractField.SYMBOL, inplace=True)
    if len(raw_option_chain) == 0:
        return None
    raw_df = pd.DataFrame(raw_option_chain)
    raw_df.index = raw_df[tda.OptionContractField.SYMBOL]
    df.reindex_like(raw_df)
    # Raw fields
    df[autoassets.OptionContractField.SYMBOL] = raw_df[tda.OptionContractField.SYMBOL]
    if tda.OptionContractField.OPEX in raw_df:
        df[autoassets.OptionContractField.OPEX] = pd.to_datetime(raw_df[tda.OptionContractField.OPEX], unit='ms', utc=True)
    if tda.OptionContractField.STRIKE in raw_df:
        df[autoassets.OptionContractField.STRIKE] = raw_df[tda.OptionContractField.STRIKE]
    if tda.OptionContractField.CONTRACT_TYPE in raw_df:
        df[autoassets.OptionContractField.CONTRACT_TYPE] = raw_df[tda.OptionContractField.CONTRACT_TYPE].apply(autoassets.OptionContractType)
    if tda.OptionContractField.DESCRIPTION in raw_df:
        df[autoassets.OptionContractField.DESCRIPTION] = raw_df[tda.OptionContractField.DESCRIPTION]
    if tda.OptionContractField.BID_PRICE in raw_df:
        df[autoassets.OptionContractField.BID_PRICE] = raw_df[tda.OptionContractField.BID_PRICE]
    if tda.OptionContractField.ASK_PRICE in raw_df:
        df[autoassets.OptionContractField.ASK_PRICE] = raw_df[tda.OptionContractField.ASK_PRICE]
    if tda.OptionContractField.LAST_PRICE in raw_df:
        df[autoassets.OptionContractField.LAST_PRICE] = raw_df[tda.OptionContractField.LAST_PRICE]
    if tda.OptionContractField.MARK_PRICE in raw_df:
        df[autoassets.OptionContractField.MARK_PRICE] = raw_df[tda.OptionContractField.MARK_PRICE]
    if tda.OptionContractField.DELTA in raw_df:
        df[autoassets.OptionContractField.DELTA] = raw_df[tda.OptionContractField.DELTA]
    if tda.OptionContractField.GAMMA in raw_df:
        df[autoassets.OptionContractField.GAMMA] = raw_df[tda.OptionContractField.GAMMA]
    if tda.OptionContractField.THETA in raw_df:
        df[autoassets.OptionContractField.THETA] = raw_df[tda.OptionContractField.THETA]
    if tda.OptionContractField.VOLATILITY in raw_df:
        df[autoassets.OptionContractField.VOLATILITY] = raw_df[tda.OptionContractField.VOLATILITY]
    if tda.OptionContractField.OPEN_INTEREST in raw_df:
        df[autoassets.OptionContractField.OPEN_INTEREST] = raw_df[tda.OptionContractField.OPEN_INTEREST]
    if tda.OptionContractField.VOLUME in raw_df:
        df[autoassets.OptionContractField.VOLUME] = raw_df[tda.OptionContractField.VOLUME]
    return df
#END: map_option_chain_dict

def map_quote_dict(raw_quote):
    """
    Map TDA's quote to AutoAssets quote.

    Parameters
    ----------
    raw_quote: dict
        TDA's quote.

    Returns
    -------
    dict:
        AutoAssets quote.
    """
    mapping = {
        autoassets.QuoteField.DESCRIPTION : tda.QuoteField.DESCRIPTION,
        autoassets.QuoteField.BID_PRICE   : tda.QuoteField.BID_PRICE,
        autoassets.QuoteField.ASK_PRICE   : tda.QuoteField.ASK_PRICE,
        autoassets.QuoteField.LAST_PRICE  : tda.QuoteField.LAST_PRICE,
        autoassets.QuoteField.MARK_PRICE  : tda.QuoteField.MARK_PRICE,
    }
    quote = {field: raw_quote[raw_field]
        for field,raw_field in mapping.items()
            if raw_field in raw_quote
    }
    return quote
#END: map_quote_dict

def place_market_order(account_id, instrument_type, ticker, direction, quantity):
    """
    Place a market order.

    Parameters
    ----------
    account_id: str
        Account ID.

    instrument_type: InstrumentType(Enum)
        Instrument type.

    ticker: str
        Trading ticker.

    direction: autoassets.OrderDirection(Enum)
        Trade direction.

    quantity: int
        Number of units to trade.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    if not autoassets.schedule.is_now_normal_market_hours():
        return False
    tda_direction = None
    if direction == autoassets.OrderDirection.BUY:
        tda_direction = tda.api.OrderDirection.BUY
    elif direction == autoassets.OrderDirection.SELL:
        tda_direction = tda.api.OrderDirection.SELL
    elif direction == autoassets.OrderDirection.BUY_TO_OPEN:
        tda_direction = tda.api.OrderDirection.BUY_TO_OPEN
    elif direction == autoassets.OrderDirection.SELL_TO_OPEN:
        tda_direction = tda.api.OrderDirection.SELL_TO_OPEN
    elif direction == autoassets.OrderDirection.BUY_TO_CLOSE:
        tda_direction = tda.api.OrderDirection.BUY_TO_CLOSE
    elif direction == autoassets.OrderDirection.SELL_TO_CLOSE:
        tda_direction = tda.api.OrderDirection.SELL_TO_CLOSE
    tda.api.post_order(account_id, {
        'orderType': tda.api.OrderType.MARKET.value,
        'session': tda.api.OrderSession.NORMAL.value,
        'duration': tda.api.OrderDuration.DAY.value,
        'orderStrategyType': tda.api.OrderStrategyType.SINGLE.value,
        'orderLegCollection': [
          {
            'instruction': tda_direction.value,
            'quantity': quantity,
            'instrument': {
                'symbol': ticker,
                'assetType': instrument_type.value,
            },
          },
        ],
    })
    #XXX: Assuming market order gets filled. Check with API to verify.
    #time.sleep(0.1) #XXX: Workaround: Give broker sufficient time to receive order.
    return True
#END: place_market_order

def place_multi_leg_market_order(account_id, instrument_type, leg_orders):
    """
    Place a market order for a multi-legged position.

    Parameters
    ----------
    account_id: str
        Account ID.

    instrument_type: InstrumentType(Enum)
        Instrument type.

    leg_orders: list(dict)
        List of leg order specifications.

    Returns
    -------
    bool:
        True if trade executed; False otherwise.
    """
    if not autoassets.schedule.is_now_normal_market_hours():
        return False
    tda_legs = []
    for leg_order in leg_orders:
        tda_direction = None
        if leg_order['direction'] == autoassets.OrderDirection.BUY:
            tda_direction = tda.api.OrderDirection.BUY
        elif leg_order['direction'] == autoassets.OrderDirection.SELL:
            tda_direction = tda.api.OrderDirection.SELL
        elif leg_order['direction'] == autoassets.OrderDirection.BUY_TO_OPEN:
            tda_direction = tda.api.OrderDirection.BUY_TO_OPEN
        elif leg_order['direction'] == autoassets.OrderDirection.SELL_TO_OPEN:
            tda_direction = tda.api.OrderDirection.SELL_TO_OPEN
        elif leg_order['direction'] == autoassets.OrderDirection.BUY_TO_CLOSE:
            tda_direction = tda.api.OrderDirection.BUY_TO_CLOSE
        elif leg_order['direction'] == autoassets.OrderDirection.SELL_TO_CLOSE:
            tda_direction = tda.api.OrderDirection.SELL_TO_CLOSE
        tda_legs.append({
            'instruction': tda_direction.value,
            'quantity': leg_order['quantity'],
            'instrument': {
                'symbol': leg_order['symbol'],
                'assetType': instrument_type.value,
            },
        })
    tda.api.post_order(account_id, {
        'orderType': tda.api.OrderType.MARKET.value,
        #'orderType': tda.api.OrderType.NET_DEBIT.value,
        #'price': 0.01,
        'session': tda.api.OrderSession.NORMAL.value,
        'duration': tda.api.OrderDuration.DAY.value,
        'orderStrategyType': tda.api.OrderStrategyType.SINGLE.value,
        'orderLegCollection': tda_legs,
    })
    #XXX: Assuming market order gets filled. Check with API to verify.
    #time.sleep(0.1) #XXX: Workaround: Give broker sufficient time to receive order.
    return True
#END: place_multi_leg_market_order

def subscribe_to_historical_data(socket, instrument_db, instruments, cb_functions=[], cb_data=[]):
    """
    Link instrument database to TDA's streaming API.

    Parameters
    ----------
    socket: dict
        Stream socket specifications.

    instrument_db: dict
        Instrument database.

    instruments: [dict]
        List of instruments.

    cb_functions: [function] (optional)
        Additional functions that will handle updates from service.

    cb_data: [any] (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    tick_instruments  = [instrument for instrument in instruments if instrument['frame'] == autoassets.InstrumentFrame.BY_TICK]
    other_instruments = [instrument for instrument in instruments if instrument not in tick_instruments]
    if len(other_instruments) > 0:
        tda.streaming.ws_subscribe_to_chart_equity(
            socket,
            symbols = autoassets.get_tickers_from_instruments(other_instruments),
            cb_functions = [_cb_patch_instrument_history],
            cb_data = [{
                'instrument_db': instrument_db,
                'instruments': other_instruments,
                'cb_functions': cb_functions,
                'cb_data': cb_data,
            }],
        )
    if len(tick_instruments) > 0:
        tda.streaming.ws_subscribe_to_timesale_equity(
            socket,
            symbols = autoassets.get_tickers_from_instruments(tick_instruments),
            cb_functions = [_cb_patch_instrument_history],
            cb_data = [{
                'instrument_db': instrument_db,
                'instruments': tick_instruments,
                'cb_functions': cb_functions,
                'cb_data': cb_data,
            }],
        )
#END: subscribe_to_historical_data

def subscribe_to_option_data(socket, option_chain_db, instruments, quote_db, cb_functions=[], cb_data=[]):
    """
    Link option chain database to TDA's streaming API.

    Parameters
    ----------
    socket: dict
        Stream socket specifications.

    option_chain_db: dict
        Option chain database.

    instruments: [dict]
        List of underlying instruments.

    quote_db: dict
        Quote database.

    cb_functions: [function] (optional)
        Additional functions that will handle updates from service.

    cb_data: [any] (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    contract_ticker_db = autoassets.get_tickers_from_option_chains(option_chain_db, instruments)
    for underlying_ticker, contract_tickers in contract_ticker_db.items():
        if len(contract_tickers) == 0:
            continue
        tda.streaming.ws_subscribe_to_option(
            socket,
            symbols = contract_tickers,
            cb_functions = [_cb_patch_instrument_option_chain],
            cb_data = [{
                'option_chain_db': option_chain_db,
                'underlying_ticker': underlying_ticker,
                'quote_db': quote_db,
                'cb_functions': cb_functions,
                'cb_data': cb_data,
            }],
        )
#END: subscribe_to_option_data

def subscribe_to_quotes(socket, quote_db, instruments, cb_functions=[], cb_data=[]):
    """
    Link quote database to TDA's streaming API.

    Parameters
    ----------
    socket: dict
        Stream socket specifications.

    quote_db: dict
        Quote database.

    instruments: [dict]
        List of instruments.

    cb_functions: [function] (optional)
        Additional functions that will handle updates from service.

    cb_data: [any] (optional)
        Additional data to pass to the callback functions, respective of order.
    """
    tda.streaming.ws_subscribe_to_quote(
        socket,
        symbols = autoassets.get_tickers_from_instruments(instruments),
        cb_functions = [_cb_patch_instrument_quotes],
        cb_data = [{
            'quote_db': quote_db,
            'cb_functions': cb_functions,
            'cb_data': cb_data,
        }],
    )
#END: subscribe_to_quotes

#################
# LOW-LEVEL API #
#################

def _cb_patch_instrument_history(history_patches, instruments_specs):
    """
    Merge bars from the TDA CHART_EQUITY service.

    Paramters
    ---------
    history_patches: [dict]
        List of bar patch specifications.

    instruments_specs: dict('instrument_db', 'instruments')
        Instrument DB and list of instrument references.
    """
    instrument_db = instruments_specs['instrument_db']
    instruments = instruments_specs['instruments']
    patched_instruments = []
    for bar_patch in history_patches:
        for instrument in instruments:
            frame = instrument['frame']
            ticker = instrument['ticker']
            if ticker != bar_patch[tda.ChartBarField.SYMBOL]:
                continue
            patched_instruments.append({
                'frame': frame,
                'ticker': ticker,
            })
            instrument_data = instrument_db[frame][ticker]
            timestamp = pd.to_datetime(bar_patch[tda.ChartBarField.TIMESTAMP], unit='ms', utc=True)
            if frame == autoassets.InstrumentFrame.MINUTELY:
                instrument_data.loc[timestamp, autoassets.ChartBarField.OPEN_PRICE]  = bar_patch[tda.ChartBarField.OPEN_PRICE]
                instrument_data.loc[timestamp, autoassets.ChartBarField.HIGH_PRICE]  = bar_patch[tda.ChartBarField.HIGH_PRICE]
                instrument_data.loc[timestamp, autoassets.ChartBarField.LOW_PRICE]   = bar_patch[tda.ChartBarField.LOW_PRICE]
                instrument_data.loc[timestamp, autoassets.ChartBarField.CLOSE_PRICE] = bar_patch[tda.ChartBarField.CLOSE_PRICE]
                instrument_data.loc[timestamp, autoassets.ChartBarField.VOLUME]      = bar_patch[tda.ChartBarField.VOLUME]
                instrument_data.sort_index(ascending=False, inplace=True)
            elif(
                 frame == autoassets.InstrumentFrame.QUARTER_HOURLY or
                 frame == autoassets.InstrumentFrame.HALF_HOURLY or
                 frame == autoassets.InstrumentFrame.HOURLY or
                 frame == autoassets.InstrumentFrame.DAILY or
                 frame == autoassets.InstrumentFrame.WEEKLY or
                 frame == autoassets.InstrumentFrame.MONTHLY
            ):
                if frame == autoassets.InstrumentFrame.QUARTER_HOURLY:
                    frame_timestamp = timestamp.replace(minute=int(timestamp.minute / 15)*15, second=0)
                elif frame == autoassets.InstrumentFrame.HALF_HOURLY:
                    frame_timestamp = timestamp.replace(minute=int(timestamp.minute / 30)*30, second=0)
                elif frame == autoassets.InstrumentFrame.HOURLY:
                    frame_timestamp = timestamp.replace(minute=0, second=0)
                else:
                    frame_timestamp = timestamp.normalize()
                if frame_timestamp not in instrument_data.index:
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.OPEN_PRICE]   = bar_patch[tda.ChartBarField.OPEN_PRICE]
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.HIGH_PRICE]   = bar_patch[tda.ChartBarField.HIGH_PRICE]
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.LOW_PRICE]    = bar_patch[tda.ChartBarField.LOW_PRICE]
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.CLOSE_PRICE]  = bar_patch[tda.ChartBarField.CLOSE_PRICE]
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.VOLUME] = bar_patch[tda.ChartBarField.VOLUME]
                else:
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.HIGH_PRICE] = max(instrument_data[autoassets.ChartBarField.HIGH_PRICE].iloc[0], bar_patch[tda.ChartBarField.HIGH_PRICE])
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.LOW_PRICE] = min(instrument_data[autoassets.ChartBarField.LOW_PRICE].iloc[0], bar_patch[tda.ChartBarField.LOW_PRICE])
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.CLOSE_PRICE] = bar_patch[tda.ChartBarField.CLOSE_PRICE]
                    instrument_data.loc[frame_timestamp, autoassets.ChartBarField.VOLUME] = instrument_data[autoassets.ChartBarField.VOLUME].iloc[0] + bar_patch[tda.ChartBarField.VOLUME]
                instrument_data.sort_index(ascending=False, inplace=True)
            elif frame == autoassets.InstrumentFrame.BY_TICK:
                tick_idx = 0 if instrument_data.empty else (instrument_data.index[0] + 1)
                instrument_data.loc[tick_idx, autoassets.ChartBarField.OPEN_PRICE]      = bar_patch['lastPrice']
                instrument_data.loc[tick_idx, autoassets.ChartBarField.HIGH_PRICE]      = bar_patch['lastPrice']
                instrument_data.loc[tick_idx, autoassets.ChartBarField.LOW_PRICE]       = bar_patch['lastPrice']
                instrument_data.loc[tick_idx, autoassets.ChartBarField.CLOSE_PRICE]     = bar_patch['lastPrice']
                instrument_data.loc[tick_idx, autoassets.ChartBarField.VOLUME]    = bar_patch['lastSize']
                instrument_data.loc[tick_idx, autoassets.ChartBarField.TIMESTAMP] = timestamp
                instrument_data.sort_index(ascending=False, inplace=True)
            elif(
                frame == autoassets.InstrumentFrame.BY_INTRADAY_VOLUME or
                frame == autoassets.InstrumentFrame.BY_DAILY_VOLUME
            ):
                volume_interval = instrument_data.index[0] - instrument_data.index[1]
                volume = instrument_data[autoassets.ChartBarField.VOLUME].iloc[0]
                volume_idx = instrument_data.index[0]
                #logger.debug('patching bar_patch: {}; instrument_data = {}'.format(bar_patch, instrument_data))
                if abs(volume_interval - (volume + bar_patch[tda.ChartBarField.VOLUME])) >= abs(volume_interval - volume): # Make new bar_patch.
                    volume_idx += volume_interval
                    volume = 0
                    instrument_data.loc[volume_idx, autoassets.ChartBarField.OPEN_PRICE] = bar_patch[tda.ChartBarField.OPEN_PRICE]
                    instrument_data.loc[volume_idx, autoassets.ChartBarField.HIGH_PRICE] = bar_patch[tda.ChartBarField.HIGH_PRICE]
                    instrument_data.loc[volume_idx, autoassets.ChartBarField.LOW_PRICE] = bar_patch[tda.ChartBarField.LOW_PRICE]
                    instrument_data.loc[volume_idx, autoassets.ChartBarField.TIMESTAMP] = pd.to_datetime(bar_patch[tda.ChartBarField.TIMESTAMP], unit='ms', utc=True)
                    instrument_data.sort_index(ascending=False, inplace=True)
                    #logger.debug('made new volume bar_patch; instrument_data = {}'.format(instrument_data))
                instrument_data.loc[volume_idx, autoassets.ChartBarField.HIGH_PRICE] = max(instrument_data[autoassets.ChartBarField.HIGH_PRICE].iloc[0], bar_patch[tda.ChartBarField.HIGH_PRICE])
                instrument_data.loc[volume_idx, autoassets.ChartBarField.LOW_PRICE] = min(instrument_data[autoassets.ChartBarField.LOW_PRICE].iloc[0], bar_patch[tda.ChartBarField.LOW_PRICE])
                instrument_data.loc[volume_idx, autoassets.ChartBarField.CLOSE_PRICE] = bar_patch[tda.ChartBarField.CLOSE_PRICE]
                instrument_data.loc[volume_idx, autoassets.ChartBarField.VOLUME] = volume + bar_patch[tda.ChartBarField.VOLUME]
                #logger.debug('patched bar_patch; instrument_data = {}'.format(instrument_data))
            else:
                logger.critical('Unsupported frame {}.'.format(frame))
    # Chaining callbacks because `patched_instruments` is a subset of the instrument DB.
    for cb_idx, cb_function in enumerate(instruments_specs['cb_functions']):
        cb_function(patched_instruments, instruments_specs['cb_data'][cb_idx])
#END: _cb_patch_instrument_history

def _cb_patch_instrument_option_chain(option_chain_patches, option_chain_specs):
    """
    Merge option data from the TDA OPTION service.

    Paramters
    ---------
    option_chain_patches: [dict]
        List of option contract patch specifications.

    option_chain_specs: dict('option_chain_db', 'underlying_ticker', 'quote_db')
        Option chain and underlying quote DB reference.
    """
    option_chain_db = option_chain_specs['option_chain_db']
    underlying_ticker = option_chain_specs['underlying_ticker']
    quote_db = option_chain_specs['quote_db']
    quote = quote_db[underlying_ticker]
    if autoassets.QuoteField.MARK_PRICE not in quote: #XXX: Field population hack for indexes such as $SPX.X.
        quote[autoassets.QuoteField.MARK_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
        quote[autoassets.QuoteField.BID_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
        quote[autoassets.QuoteField.ASK_PRICE] = quote[autoassets.QuoteField.LAST_PRICE]
    tickers = []
    for option_chain_patch in option_chain_patches:
        #logger.debug(option_chain_patch)
        ticker = option_chain_patch[tda.OptionContractField.SYMBOL]
        tickers.append(ticker)
        mapped_option_chain_patch = map_option_chain_dict([option_chain_patch])
        option_chain_db[underlying_ticker].update(mapped_option_chain_patch)
        option_chain = option_chain_db[underlying_ticker]
        # Generated fields
        if tda.OptionContractField.VOLUME in option_chain_patch:
            #diff_quote = quote[autoassets.QuoteField.MARK_PRICE] - quote[autoassets.QuoteField.PREV_MARK_PRICE]
            prev_volume = np.where(option_chain[autoassets.OptionContractField.PREV_VOLUME] == 0, option_chain[autoassets.OptionContractField.VOLUME], option_chain[autoassets.OptionContractField.PREV_VOLUME])
            option_chain[autoassets.OptionContractField.PREV_VOLUME] = prev_volume
            sign_quote = math.copysign(1.0, quote[autoassets.QuoteField.MARK_PRICE] - quote[autoassets.QuoteField.PREV_MARK_PRICE])
            diff_volume = option_chain[autoassets.OptionContractField.VOLUME] - option_chain[autoassets.OptionContractField.PREV_VOLUME]
            option_chain[autoassets.OptionContractField.VOLUME_BIAS] += sign_quote * diff_volume
            option_chain[autoassets.OptionContractField.PREV_VOLUME] = option_chain[autoassets.OptionContractField.VOLUME]
        if tda.OptionContractField.MARK_PRICE in option_chain_patch or tda.OptionContractField.BID_PRICE in option_chain_patch or tda.OptionContractField.ASK_PRICE in option_chain_patch:
            sign = np.where(option_chain[autoassets.OptionContractField.CONTRACT_TYPE] == autoassets.OptionContractType.CALL, 1.0, -1.0)
            option_chain[autoassets.OptionContractField.EXTRINSIC_MARK_PRICE] = (option_chain[autoassets.OptionContractField.MARK_PRICE] - (sign * (quote[autoassets.QuoteField.MARK_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
            option_chain[autoassets.OptionContractField.EXTRINSIC_BID_PRICE] = (option_chain[autoassets.OptionContractField.BID_PRICE] - (sign * (quote[autoassets.QuoteField.BID_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
            option_chain[autoassets.OptionContractField.EXTRINSIC_ASK_PRICE] = (option_chain[autoassets.OptionContractField.ASK_PRICE] - (sign * (quote[autoassets.QuoteField.ASK_PRICE] - option_chain[autoassets.OptionContractField.STRIKE])).clip(lower=0.0)).clip(lower=0.0)
    quote[autoassets.QuoteField.PREV_MARK_PRICE] = quote[autoassets.QuoteField.MARK_PRICE]
    # Chaining callbacks because `tickers` is a subset of the option chain DB.
    for cb_idx, cb_function in enumerate(option_chain_specs['cb_functions']):
        cb_function(underlying_ticker, tickers, option_chain_specs['cb_data'][cb_idx])
#END: _cb_patch_instrument_option_chain

def _cb_patch_instrument_quotes(quote_patches, quote_specs):
    """
    Merge quotes from the TDA QUOTE service.

    Paramters
    ---------
    quote_patches: [dict]
        List of bar patch specifications.

    quote_specs: dict('quote_db')
        Quote DB reference.
    """
    quote_db = quote_specs['quote_db']
    tickers = []
    for quote_patch in quote_patches:
        logger.debug(quote_patch)
        ticker = quote_patch[tda.QuoteField.SYMBOL]
        tickers.append(ticker)
        mapped_quote_patch = map_quote_dict(quote_patch)
        quote_db[ticker].update(mapped_quote_patch)
    # Chaining callbacks because `tickers` is a subset of the quote DB.
    for cb_idx, cb_function in enumerate(quote_specs['cb_functions']):
        cb_function(tickers, quote_specs['cb_data'][cb_idx])
#END: _cb_patch_instrument_quotes
