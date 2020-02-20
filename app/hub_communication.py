'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import requests
from contextlib import closing

from app.utils_file_loads import get_jhubtoken
from app.utils import remove_secret

def remove_proxy_route(app_logger, uuidcode, app_hub_url_proxy_route, jhubtoken, username, server_name):
    app_logger.debug("uuidcode={} - Remove proxys from server_name, because the original host is not accessable any longer".format(uuidcode))
    hub_header = {"Authorization": "token {}".format(jhubtoken),
                  "uuidcode": uuidcode,
                  "Intern-Authorization": get_jhubtoken()}
    try:
        app_logger.info("uuidcode={} - Remove Proxys for {}".format(uuidcode, server_name))
        url = app_hub_url_proxy_route
        if ':' in server_name:
            server_name = server_name.split(':')[1]
        url = url + '/' + username
        if server_name != '':
            url = url + '/' + server_name
        app_logger.trace("uuidcode={} - Delete Proxy Route: {} {}".format(uuidcode, url, hub_header))
        for i in range(0, 10):
            with closing(requests.delete(url,
                                         headers = hub_header,
                                         verify = False,
                                         timeout = 1800)) as r:
                if r.status_code == 200:
                    app_logger.info("uuidcode={} - Proxy route deletion successful".format(uuidcode))
                    return True
                elif r.status_code == 503:
                    app_logger.info("uuidcode={} - Proxy route deletion status_code 503. Try again (Try {}/10)".format(uuidcode, i+1))
                else:
                    raise Exception("uuidcode={} - Could not remove proxy route for server_name {}: {} {}".format(uuidcode, server_name, r.text, r.status_code))
    except requests.exceptions.ConnectTimeout:
        app_logger.exception("uuidcode={} - Timeout reached (1800). Could not remove routes from proxy via JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))   
    except:
        app_logger.exception("uuidcode={} - Could not remove routes from proxy via JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))


def token(app_logger, uuidcode, app_hub_url_proxy_route, app_hub_url_token, jhubtoken, accesstoken, expire, username, server_name):
    app_logger.debug("uuidcode={} - Send new token to JupyterHub".format(uuidcode))
    app_logger.trace("uuidcode={} - Access_token: {} , expire: {}".format(uuidcode, accesstoken, expire))
    hub_header = {"Authorization": "token {}".format(jhubtoken),
                  "uuidcode": uuidcode,
                  "Intern-Authorization": get_jhubtoken()}
    hub_json = {"accesstoken": accesstoken,
                "expire": str(expire)}
    try:
        app_logger.info("uuidcode={} - Update JupyterHub Token".format(uuidcode))
        url = app_hub_url_token
        if ':' in server_name:
            server_name = server_name.split(':')[1]
        url = url + '/' + username
        if server_name != '':
            url = url + '/' + server_name
        app_logger.trace("uuidcode={} - Update JupyterHub Token: {} {} {}".format(uuidcode, url, hub_header, hub_json))
        with closing(requests.post(url,
                                   headers = hub_header,
                                   json = hub_json,
                                   verify = False,
                                   timeout = 1800)) as r:
            if r.status_code == 201:
                app_logger.trace("uuidcode={} - Token Update successful: {} {} {}".format(uuidcode, r.text, r.status_code, r.headers))
                return
            elif r.status_code == 503:
                remove_proxy_route(app_logger,
                                   uuidcode,
                                   app_hub_url_proxy_route,
                                   jhubtoken,
                                   username,
                                   server_name)
                # try again
                with closing(requests.post(url,
                                           headers = hub_header,
                                           json = hub_json,
                                           verify = False,
                                           timeout = 1800)) as r2:
                    if r2.status_code == 201:
                        app_logger.trace("uuidcode={} - Token Update successful: {} {} {}".format(uuidcode, r2.text, r2.status_code, r2.headers))
                        return
                    else:
                        app_logger.warning("uuidcode={} - Token Update sent wrong status_code: {} {} {}".format(uuidcode, r2.text, r2.status_code, remove_secret(r2.headers)))
            else:
                app_logger.warning("uuidcode={} - Token Update sent wrong status_code: {} {} {}".format(uuidcode, r.text, r.status_code, remove_secret(r.headers)))
    except requests.exceptions.ConnectTimeout:
        app_logger.exception("uuidcode={} - Timeout reached (1800). Could not send update token to JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))
    except:
        app_logger.exception("uuidcode={} - Could not send update token to JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))
    
def status(app_logger, uuidcode, app_hub_url_proxy_route, app_hub_url_status, jhubtoken, status, username, server_name=''):
    app_logger.debug("uuidcode={} - Send status to JupyterHub: {}".format(uuidcode, status))
    hub_header = {"Authorization": "token {}".format(jhubtoken),
                  "uuidcode": uuidcode,
                  "Intern-Authorization": get_jhubtoken()}
    hub_json = {
        "Status": status
        }
    try:
        url = app_hub_url_status
        if ':' in server_name:
            server_name = server_name.split(':')[1]
        url = url + '/' + username
        if server_name != '':
            url = url + '/' + server_name
        app_logger.trace("uuidcode={} - Update JupyterHub Status: {} {} {}".format(uuidcode, url, hub_header, hub_json))
        with closing(requests.post(url,
                                   headers = hub_header,
                                   json = hub_json,
                                   verify = False,
                                   timeout = 1800)) as r:
            if r.status_code == 201:
                app_logger.trace("uuidcode={} - Status Update successful: {} {} {}".format(uuidcode, r.text, r.status_code, r.headers))
                return
            elif r.status_code == 503:
                remove_proxy_route(app_logger,
                                   uuidcode,
                                   app_hub_url_proxy_route,
                                   jhubtoken,
                                   username,
                                   server_name)
                # try again
                with closing(requests.post(url,
                                           headers = hub_header,
                                           json = hub_json,
                                           verify = False,
                                           timeout = 1800)) as r2:
                    if r2.status_code == 201:
                        app_logger.trace("uuidcode={} - Status Update successful: {} {} {}".format(uuidcode, r2.text, r2.status_code, r2.headers))
                        return
                    elif r2.status_code == 404:
                        app_logger.info("uuidcode={} - JupyterHub doesn't know the spawner.".format(uuidcode))
            elif r.status_code == 404:
                app_logger.info("uuidcode={} - JupyterHub doesn't know the spawner.".format(uuidcode))
    except requests.exceptions.ConnectTimeout:
        app_logger.exception("uuidcode={} - Timeout reached (1800). Could not send status update to JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))
    except:
        app_logger.exception("uuidcode={} - Could not send status update to JupyterHub. Url: {} Headers: {}".format(uuidcode, url, remove_secret(hub_header)))

def cancel(app_logger, uuidcode, app_hub_url_proxy_route, app_hub_url_cancel, jhubtoken, errormsg, username, server_name=''):
    app_logger.debug("uuidcode={} - Send cancel to JupyterHub".format(uuidcode))
    hub_header = {"Authorization": "token {}".format(jhubtoken),
                  "Intern-Authorization": get_jhubtoken(),
                  "uuidcode": uuidcode,
                  "Error": errormsg,
                  "Stopped": "True"}
    try:
        url = app_hub_url_cancel
        if ':' in server_name:
            server_name = server_name.split(':')[1]
        url = url + '/' + username
        if server_name != '':
            url = url + '/' + server_name
        app_logger.trace("uuidcode={} - Cancel JupyterHub: {} {}".format(uuidcode, url, hub_header))
        with closing(requests.delete(url,
                                     headers = hub_header, 
                                     verify = False,
                                     timeout = 1800)) as r:
            if r.status_code == 202:
                app_logger.trace("uuidcode={} - Cancel successful: {} {} {}".format(uuidcode, r.text, r.status_code, r.headers))
                return        
            elif r.status_code == 503:
                remove_proxy_route(app_logger,
                                   uuidcode,
                                   app_hub_url_proxy_route,
                                   jhubtoken,
                                   username,
                                   server_name)
                # try again
                with closing(requests.delete(url,
                                             headers = hub_header,
                                             verify = False,
                                             timeout = 1800)) as r2:
                    if r2.status_code == 202:
                        app_logger.trace("uuidcode={} - Cancel successful: {} {} {}".format(uuidcode, r2.text, r2.status_code, r2.headers))
                        return
                    else:
                        app_logger.warning("uuidcode={} - JupyterHub.cancel sent wrong status_code: {} {} {}".format(uuidcode, r2.text, r2.status_code, remove_secret(r2.headers)))
            else:
                app_logger.warning("uuidcode={} - JupyterHub.cancel sent wrong status_code: {} {} {}".format(uuidcode, r.text, r.status_code, remove_secret(r.headers)))
    except requests.exceptions.ConnectTimeout:
        app_logger.exception("uuidcode={} - Timeout reached (1800). Could not send cancel to JupyterHub. Url: {}, Headers: {}".format(uuidcode, url, remove_secret(hub_header)))
    except:
        if errormsg != "":
            cancel(app_logger, uuidcode, app_hub_url_proxy_route, app_hub_url_cancel, jhubtoken, "", username, server_name)
        else:
            app_logger.exception("uuidcode={} - Could not send cancel to JupyterHub. Url: {}, Headers: {}".format(uuidcode, url, remove_secret(hub_header)))
