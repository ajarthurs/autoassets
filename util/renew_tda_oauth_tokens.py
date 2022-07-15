"""
Renew OAuth tokens.

Opens a web browser for the user to grant this application access and then cache the new tokens for later use. Note that the code returned after authorization is url-encoded and should be entered as-is at the prompt. The access token is the authorization key required to access the TDA endpoints; however, it is short-lived. The refresh token, which is long-lived, is used to renew the access token. (See https://developer.tdameritrade.com/content/phase-1-authentication-update-xml-based-api).
"""

import autoassets.backend.tda
import logging
import logging.config
import subprocess
import tda.api
import yaml
from settings import backends_setting

logger = logging.getLogger(__file__)

def main():
    settings = backends_setting[autoassets.backend.tda]
    app_key = settings['app_key']
    app_redirect_url = settings['app_redirect_url']
    subprocess.run([
        '/usr/bin/firefox',
        '--new-window',
        tda.api.build_oauth_url(app_key, app_redirect_url),
        ])

    code = input('Enter response code (look for `code=` in query string): ')
    oauth_token_dict = tda.api.renew_oauth_tokens(app_key, app_redirect_url, code)
    tda.api.cache_oauth_tokens(oauth_token_dict)

    logger.info('Successfully renewed tokens.')
#END: main

if(__name__ == '__main__'):
    with open('logging.yaml') as f:
        y = yaml.load(f, yaml.FullLoader)
        logging.config.dictConfig(y)
    main()
