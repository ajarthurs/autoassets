"""
AutoAssets settings.
"""

import autoassets
import autoassets.backend.tda
import autoassets.positioning.accumulative
import autoassets.positioning.ratio2
import autoassets.strategy.buy
import autoassets.strategy.trend
from datetime import time
from pytz import timezone

# Per-backend settings.
backends_setting = {
    autoassets.backend.tda: {
        # TDA broker account.
        'account_id': '[MY BROKERAGE ACCOUNT ID]',
        # Developer App.
        'app_name': 'AutoAssets',
        'app_key': '[MY APP KEY]',
        'app_redirect_url': '[MY APP CALLBACK URL]',
        'qos': autoassets.backend.tda.WSQOSLevel.D5000MS,
    },
}

# Blueprint of assets.
assets_definition = {
    'Trend A': {
        'positioning': {
            'structure': autoassets.positioning.accumulative,
            'backend': autoassets.backend.tda,
            'instrument_type': autoassets.backend.tda.InstrumentType.EQUITY,
            'ticker': {'ref': 'ticker'},
            'enable_trades': {'ref': 'enable_trades', 'default': False},
            'optionable': {'ref': 'optionable', 'default': False},
            'expected_rate': {'ref': 'expected_rate'},
        },
        'strategy': {
            'method': autoassets.strategy.trend,
            'instrument': {
                'backend': autoassets.backend.tda,
                'frame': autoassets.InstrumentFrame.BY_INTRADAY_VOLUME,
                'ticker': {'ref': 'ticker'},
            },
            'neutralize_on_close': False,
        },
    },
    'BWB A': {
        'positioning': {
            'structure': autoassets.positioning.ratio2,
            'backend': autoassets.backend.tda,
            'instrument_type': autoassets.backend.tda.InstrumentType.OPTION,
            'ticker': {'ref': 'ticker'},
            'optionable': {'ref': 'optionable', 'default': True},
            'enable_trades': {'ref': 'enable_trades', 'default': False},
            'denomination': {'ref': 'denomination', 'default': 1},
            'target_premium_per_day': {'ref': 'target_premium_per_day'},
            'max_dte': 3,
            'strike_count': 200,
        },
        'strategy': {
            'method': autoassets.strategy.buy,
            'start_at': time(9, 35, 0, 0, tzinfo=timezone('America/New_York')),
            'neutralize_at': time(9, 40, 0, 0, tzinfo=timezone('America/New_York')),
        },
    },
}
assets_path = 'assets.json'
