'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import base64
import time
import requests
from contextlib import closing

from app.utils_file_loads import get_unity
from app import hub_communication
from app.utils import remove_secret


def renew_token(app_logger, uuidcode, refreshtoken, accesstoken, expire, jhubtoken, app_hub_url_proxy_route, app_hub_url_token, username, servername=''):
    if int(expire) - time.time() > 60:
        return accesstoken, expire
    app_logger.info("{} - Renew Token".format(uuidcode))
    unity = get_unity()
    b64key = base64.b64encode(bytes('{}:{}'.format(unity.get('client_id'), unity.get('client_secret')), 'utf-8')).decode('utf-8')
    data = {'refresh_token': refreshtoken,
            'grant_type': 'refresh_token',
            'scope': ' '.join(unity.get('scope'))}
    headers = { 'Authorization': 'Basic {}'.format(b64key),
                'Accept': 'application/json' }
    app_logger.info("{} - Post to {}".format(uuidcode, unity.get('links').get('token')))
    app_logger.trace("{} - Header: {}".format(uuidcode, headers))
    app_logger.trace("{} - Data: {}".format(uuidcode, data))
    try:
        with closing(requests.post(unity.get('links').get('token'),
                                   headers = headers,
                                   data = data,
                                   verify = unity.get('certificate', False))) as r:
            app_logger.trace("{} - Unity Response: {} {} {} {}".format(uuidcode, r.text, r.status_code, remove_secret(r.headers), remove_secret(r.json)))
            accesstoken = r.json().get('access_token')
        with closing(requests.get(unity.get('links').get('tokeninfo'),
                                  headers = { 'Authorization': 'Bearer {}'.format(accesstoken) },
                                  verify=unity.get('certificate', False))) as r:
            app_logger.trace("{} - Unity Response: {} {} {} {}".format(uuidcode, r.text, r.status_code, remove_secret(r.headers), remove_secret(r.json)))
            expire = r.json().get('exp')
    except:
        app_logger.exception("{} - Could not update token".format(uuidcode))
        raise Exception("{} - Could not update token".format(uuidcode))
    app_logger.info("{} - Update JupyterHub Token".format(uuidcode))
    hub_communication.token(app_logger,
                            uuidcode,
                            app_hub_url_proxy_route,
                            app_hub_url_token,
                            jhubtoken,
                            accesstoken,
                            expire,
                            username,
                            servername)
    return accesstoken
