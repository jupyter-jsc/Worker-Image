'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import time
import json

from app import unicore_communication, hub_communication,\
    tunnel_utils, orchestrator_communication
from app.utils import remove_secret
from app.jobs_utils import stop_job

def get(app_logger, uuidcode, request_headers, unicore_header, app_urls, cert):
    servername = request_headers.get('servername')
    if ':' in servername:
        servername = servername.split(':')[1]
    else:
        servername = ''
    counter = 0
    children = []
    status = ''
    while True:
        # start with sleep, this function is only called, if .host was not in children
        time.sleep(3)

        for i in range(3):  # @UnusedVariable
            properties_json = {}
            try:
                method = "GET"
                method_args = {"url": request_headers.get('kernelurl'),
                               "headers": unicore_header,
                               "certificate": cert}
                app_logger.info("{} - Get Properties of UNICORE/X Job {}".format(uuidcode, request_headers.get('kernelurl')))
                text, status_code, response_header = unicore_communication.request(app_logger,
                                                                                   uuidcode,
                                                                                   method,
                                                                                   method_args)
                if status_code == 200:
                    unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                    properties_json = json.loads(text)
                    if properties_json.get('status') == 'UNDEFINED' and i < 4:
                        app_logger.debug("{} - Received status UNDEFINED. Try again in 2 seconds".format(uuidcode))
                        time.sleep(2)
                    else:
                        break
                elif status_code == 404:
                    if i < 4:
                        app_logger.debug("{} - Could not get properties. 404 Not found. Sleep for 2 seconds and try again".format(uuidcode))
                        time.sleep(2)
                    else:
                        app_logger.warning("{} - Could not get properties. 404 Not found. Do nothing and return. {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                        return "", 539
                else:
                    if i < 4:
                        app_logger.debug("{} - Could not get properties. Sleep for 2 seconds and try again".format(uuidcode))
                        time.sleep(2)
                    else:
                        app_logger.warning("{} - Could not get properties. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                        raise Exception("{} - Could not get properties. Throw exception because of wrong status_code: {}".format(uuidcode, status_code))
            except:
                app_logger.exception("{} - Could not get properties. Try to stop it {} {}".format(uuidcode, method, remove_secret(method_args)))
                app_logger.trace("{} - Call stop_job".format(uuidcode))
                stop_job(app_logger,
                         uuidcode,
                         servername,
                         request_headers,
                         app_urls)
                return "", 539

        if properties_json.get('status') in ['SUCCESSFUL', 'ERROR', 'FAILED', 'NOT_SUCCESSFUL']:
            # Job is Finished for UNICORE, so it should be for JupyterHub
            app_logger.warning('{} - Get: Job is finished or failed - JobStatus: {}. Send Information to JHub'.format(uuidcode, properties_json.get('status')))
            app_logger.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app_logger,
                     uuidcode,
                     servername,
                     request_headers,
                     app_urls)
            return "", 530
        
        try:
            method = "GET"
            method_args = {"url": request_headers.get('filedir'),
                           "headers": unicore_header,
                           "certificate": cert} 
            text, status_code, response_header = unicore_communication.request(app_logger,
                                                                               uuidcode,
                                                                               method,
                                                                               method_args)
            if status_code == 200:
                unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                children = json.loads(text).get('children', [])
            elif status_code == 404:
                app_logger.warning("{} - Could not get properties. 404 Not found. Do nothing and return. {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                return "", 539
            else:
                app_logger.warning("{} - Could not get information about filedirectory. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                raise Exception("{} - Could not get information about filedirectory. Throw Exception because of wrong status_code: {}".format(uuidcode, status_code))
        except:
            counter += 1
            if counter > 10:
                app_logger.error("{} - Get filelist ({}) failed 10 times over 30 seconds. {} {}".format(uuidcode, request_headers.get('filedir'), method, remove_secret(method_args)))
                app_logger.trace("{} - Call stop_job".format(uuidcode))
                stop_job(app_logger,
                         uuidcode,
                         servername,
                         request_headers,
                         app_urls)
            app_logger.info("{} - Get filelist ({}) failed {} time(s)".format(uuidcode, request_headers.get('filedir'), counter))
            hub_communication.status(app_logger,
                                     uuidcode,
                                     app_urls.get('hub', {}).get('url_proxy_route'),
                                     app_urls.get('hub', {}).get('url_status'),
                                     request_headers.get('jhubtoken'),
                                     'waitforhostname',
                                     request_headers.get('escapedusername'),
                                     servername)
            continue
        if '.end' in children or '/.end' in children:
            # It's not running anymore
            status = 'stopped'
        elif '.host' in children or '/.host' in children:
            # running, build up tunnel
            try:
                tunnel_utils.create(app_logger,
                                    uuidcode,
                                    app_urls.get('hub', {}).get('url_proxy_route'),
                                    app_urls.get('tunnel', {}).get('url_tunnel'),
                                    app_urls.get('hub', {}).get('url_cancel'),
                                    request_headers.get('kernelurl'),
                                    request_headers.get('filedir'),
                                    unicore_header,
                                    request_headers.get('servername'),
                                    request_headers.get('system'),
                                    request_headers.get('port'),
                                    cert,
                                    request_headers.get('jhubtoken'),
                                    request_headers.get('escapedusername'),
                                    servername)
            except:
                app_logger.exception("{} - Could not create tunnel".format(uuidcode))
                app_logger.trace("{} - Call stop_job".format(uuidcode))
                stop_job(app_logger,
                         uuidcode,
                         servername,
                         request_headers,
                         app_urls)
                return
            status = "running"
        else:
            app_logger.info("{} - Update JupyterHub status ({})".format(uuidcode, "waitforhostname"))
            hub_communication.status(app_logger,
                                     uuidcode,
                                     app_urls.get('hub', {}).get('url_proxy_route'),
                                     app_urls.get('hub', {}).get('url_status'),
                                     request_headers.get('jhubtoken'),
                                     "waitforhostname",
                                     request_headers.get('escapedusername'),
                                     servername)
            continue
        app_logger.info("{} - Update JupyterHub status ({})".format(uuidcode, status))
        hub_communication.status(app_logger,
                                 uuidcode,
                                 app_urls.get('hub', {}).get('url_proxy_route'),
                                 app_urls.get('hub', {}).get('url_status'),
                                 request_headers.get('jhubtoken'),
                                 status,
                                 request_headers.get('escapedusername'),
                                 servername)
        if status in ['running', 'stopped'] and request_headers.get('spawning', 'true').lower() == 'true': # spawning is finished
            app_logger.trace('{} - Tell J4J_Orchestrator that the spawning is done'.format(uuidcode))
            try:
                orchestrator_communication.set_spawning(app_logger,
                                                        uuidcode,
                                                        app_urls.get('orchestrator', {}).get('url_spawning'),
                                                        request_headers.get('servername'),
                                                        'False')
            except:
                app_logger.exception("{} - Could not set spawning to false in J4J_Orchestrator database for {}".format(uuidcode, request_headers.get('servername')))
        return
