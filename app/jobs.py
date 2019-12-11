'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import json

from threading import Thread
from flask import request
from flask_restful import Resource
from flask import current_app as app

from app.utils import validate_auth, remove_secret
from app.jobs_utils import stop_job
from app import unicore_utils, utils_file_loads, unicore_communication,\
    hub_communication, tunnel_utils, jobs_threads, orchestrator_communication
from time import sleep

class Jobs(Resource):
    def get(self):
        # Track actions through different webservices.
        uuidcode = request.headers.get('uuidcode', '<no uuidcode>')
        app.log.info("{} - Get Server Status".format(uuidcode))
        app.log.trace("{} - Headers: {}".format(uuidcode, request.headers.to_list()))

        # Check for J4J intern token
        validate_auth(app.log,
                      uuidcode,
                      request.headers.get('intern-authorization'))

        servername = request.headers.get('servername')

        # Create UNICORE header and get certificate
        unicore_header, accesstoken, expire = unicore_utils.create_header(app.log,     # @UnusedVariable
                                                                          uuidcode,
                                                                          request.headers,
                                                                          app.urls.get('hub', {}).get('url_proxy_route'),
                                                                          app.urls.get('hub', {}).get('url_token'),
                                                                          request.headers.get('escapedusername'),
                                                                          servername)
        app.log.trace("{} - FileLoad: UNICORE/X certificate path".format(uuidcode))
        cert = utils_file_loads.get_unicore_certificate()
        app.log.trace("{} - FileLoad: UNICORE/X certificate path Result: {}".format(uuidcode, cert))

        # Get Properties of kernelurl
        kernelurl = request.headers.get('kernelurl')
        for i in range(5):  # @UnusedVariable
            properties_json = {}
            try:
                method = "GET"
                method_args = {"url": kernelurl,
                               "headers": unicore_header,
                               "certificate": cert}
                app.log.info("{} - Get Properties of UNICORE/X Job {}".format(uuidcode, kernelurl))
                text, status_code, response_header = unicore_communication.request(app.log,
                                                                                   uuidcode,
                                                                                   method,
                                                                                   method_args)
                if status_code == 200:
                    unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                    properties_json = json.loads(text)
                    if properties_json.get('status') == 'UNDEFINED' and i < 4:
                        app.log.debug("{} - Received status UNDEFINED. Try again in 2 seconds".format(uuidcode))
                        sleep(2)
                    else:
                        break
                elif status_code == 404:
                    if i < 4:
                        app.log.debug("{} - Could not get properties. 404 Not found. Sleep for 2 seconds and try again".format(uuidcode))
                        sleep(2)
                    else:
                        app.log.warning("{} - Could not get properties. 404 Not found. Do nothing and return. {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                        return "", 539
                else:
                    if i < 4:
                        app.log.debug("{} - Could not get properties. Sleep for 2 seconds and try again".format(uuidcode))
                        sleep(2)
                    else:
                        app.log.warning("{} - Could not get properties. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                        raise Exception("{} - Could not get properties. Throw exception because of wrong status_code: {}".format(uuidcode, status_code))
            except:
                app.log.exception("{} - Could not get properties. JupyterLab will be still running. {} {}".format(uuidcode, method, remove_secret(method_args)))
                app.log.warning("{} - Do not send update to JupyterHub.".format(uuidcode))
                # If JupyterHub don't receives an update for a long time it can stop the job itself.
                return "", 539

        if properties_json.get('status') in ['SUCCESSFUL', 'ERROR', 'FAILED', 'NOT_SUCCESSFUL']:
            # Job is Finished for UNICORE, so it should be for JupyterHub
            app.log.warning('{} - Get: Job is finished or failed - JobStatus: {}. Send Information to JHub'.format(uuidcode, properties_json.get('status')))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)
            return "", 530

        # The Job is not finished yet (good)
        # Get Files in the filedir
        children = []
        try:
            method = "GET"
            method_args = {"url": request.headers.get('filedir'),
                           "headers": unicore_header,
                           "certificate": cert}
            app.log.info("{} - Get list of files of UNICORE/X Job {}".format(uuidcode, kernelurl))
            text, status_code, response_header = unicore_communication.request(app.log,
                                                                               uuidcode,
                                                                               method,
                                                                               method_args)
            if status_code == 200:
                unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                children = json.loads(text).get('children', [])
            elif status_code == 404:
                app.log.warning("{} - Could not get properties. 404 Not found. Do nothing and return. {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                return "", 539
            else:
                app.log.warning("{} - Could not get information about filedirectory. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                raise Exception("{} - Could not get information about filedirectory. Throw Exception because of wrong status_code: {}".format(uuidcode, status_code))
        except:
            app.log.exception("{} - Could not get information about filedirectory. {} {}".format(uuidcode, method, remove_secret(method_args)))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)
            return "", 539


        # get the 'real' status of the job from the files in the working_directory
        # 'real' means: We don't care about Queued, ready, running or something. We just want to know: Is it bad (failed or cancelled) or good (running or spawning)
        status = ''
        if properties_json.get('status') in ['QUEUED', 'READY', 'RUNNING', 'STAGINGIN']:
            if '.end' in children or '/.end' in children:
                # It's not running anymore
                status = 'stopped'
            elif '.tunnel' in children or '/.tunnel' in children:
                # It's running and tunnel is up
                status = 'running'
            elif '.host' in children or '/.host' in children:
                # running, build up tunnel
                try:
                    tunnel_utils.create(app.log,
                                        uuidcode,
                                        app.urls.get('hub', {}).get('url_proxy_route'),
                                        app.urls.get('tunnel', {}).get('url_tunnel'),
                                        app.urls.get('hub', {}).get('url_cancel'),
                                        kernelurl,
                                        request.headers.get('filedir'),
                                        unicore_header,
                                        request.headers.get('servername'),
                                        request.headers.get('system'),
                                        request.headers.get('port'),
                                        cert,
                                        request.headers.get('jhubtoken'),
                                        request.headers.get('escapedusername'),
                                        servername)
                except:
                    app.log.error("{} - Could not create Tunnel. Used Parameters: {} {} {} {} {} {} {} {} {} {} {} {}".format(uuidcode,
                                                                                                                              app.urls.get('tunnel', {}).get('url_tunnel'),
                                                                                                                              app.urls.get('hub', {}).get('url_cancel'),
                                                                                                                              kernelurl,
                                                                                                                              request.headers.get('filedir'),
                                                                                                                              remove_secret(unicore_header),
                                                                                                                              request.headers.get('servername'),
                                                                                                                              request.headers.get('system'),
                                                                                                                              request.headers.get('port'),
                                                                                                                              cert,
                                                                                                                              '<secret>'))
                    app.log.trace("{} - Call stop_job".format(uuidcode))
                    stop_job(app.log,
                             uuidcode,
                             servername,
                             request.headers,
                             app.urls)
                    return "", 539
                status = 'running'
            else:
                request_headers = {}
                for key, value in request.headers.items():
                    if 'Token' in key:
                        key = key.replace('-', '_')
                    request_headers[key.lower()] = value
                app.log.trace("{} - New Header for Thread: {}".format(uuidcode, request_headers))
                # no .host in children, let's start a thread which looks for it every second
                t = Thread(target=jobs_threads.get,
                           args=(app.log,
                                 uuidcode,
                                 request_headers,
                                 unicore_header,
                                 app.urls,
                                 cert))
                t.start()
                status = 'waitforhostname'
            app.log.info("{} - Update JupyterHub status ({})".format(uuidcode, status))
            hub_communication.status(app.log,
                                     uuidcode,
                                     app.urls.get('hub', {}).get('url_proxy_route'),
                                     app.urls.get('hub', {}).get('url_status'),
                                     request.headers.get('jhubtoken'),
                                     status,
                                     request.headers.get('escapedusername'),
                                     servername)
            if status in ['running', 'stopped'] and request.headers.get('spawning', 'true').lower() == 'true': # spawning is finished
                app.log.trace('{} - Tell J4J_Orchestrator that the spawning is done'.format(uuidcode))
                try:
                    orchestrator_communication.set_spawning(app.log,
                                                            uuidcode,
                                                            app.urls.get('orchestrator', {}).get('url_spawning'),
                                                            request.headers.get('servername'),
                                                            'False')
                except:
                    app.log.exception("{} - Could not set spawning to false in J4J_Orchestrator database for {}".format(uuidcode, request_headers.get('servername')))

        else:
            app.log.warning('{} - Unknown JobStatus: {}'.format(uuidcode, properties_json.get('status')))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)

    def post(self):
        # Track actions through different webservices.
        uuidcode = request.headers.get('uuidcode', '<no uuidcode>')
        app.log.info("{} - Spawn Server".format(uuidcode))
        app.log.trace("{} - Headers: {}".format(uuidcode, request.headers.to_list()))
        app.log.trace("{} - Json: {}".format(uuidcode, request.json))

        # Check for J4J intern token
        validate_auth(app.log,
                      uuidcode,
                      request.headers.get('Intern-Authorization'))

        servername = request.headers.get('servername')
        # Create header for unicore job
        unicore_header, accesstoken, expire = unicore_utils.create_header(app.log,  # @UnusedVariable
                                                                          uuidcode,
                                                                          request.headers,
                                                                          app.urls.get('hub', {}).get('url_proxy_route'),
                                                                          app.urls.get('hub', {}).get('url_token'),
                                                                          request.headers.get('escapedusername'),
                                                                          servername)


        # Create input files for the job. A working J4J_tunnel webservice is required
        try:
            unicore_input = unicore_utils.create_inputs(app.log,
                                                        uuidcode,
                                                        request.json,
                                                        app.urls.get('tunnel', {}).get('url_remote'))
        except:
            app.log.exception("{} - Could not create input files for UNICORE/X Job. {} {}".format(uuidcode, remove_secret(request.json), app.urls.get('tunnel', {}).get('url_remote')))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)
            return "", 534

        # Create Job description
        unicore_json = unicore_utils.create_job(app.log,
                                                uuidcode,
                                                request.json,
                                                request.headers.get('Project'),
                                                unicore_input)

        # Get URL and certificate to communicate with UNICORE/X
        app.log.trace("{} - FileLoad: UNICORE/X url".format(uuidcode))
        urls = utils_file_loads.get_unicorex_urls()
        app.log.trace("{} - FileLoad: UNICORE/X url Result: {}".format(uuidcode, urls))
        url = urls.get(request.json.get('system'))

        app.log.trace("{} - FileLoad: UNICORE/X certificate path".format(uuidcode))
        cert = utils_file_loads.get_unicore_certificate()
        app.log.trace("{} - FileLoad: UNICORE/X certificate path Result: {}".format(uuidcode, cert))

        # Submit Job. It will not be started, because of unicore_json['haveClientStageIn']='true'
        kernelurl = ""
        try:
            hub_communication.status(app.log,
                                     uuidcode,
                                     app.urls.get('hub', {}).get('url_proxy_route'),
                                     app.urls.get('hub', {}).get('url_status'),
                                     request.headers.get('jhubtoken'),
                                     'submitunicorejob',
                                     request.headers.get('escapedusername'),
                                     servername)
            method = "POST"
            method_args = {"url": url + "/jobs",
                           "headers": unicore_header,
                           "data": json.dumps(unicore_json),
                           "certificate": cert}
            app.log.info("{} - Submit UNICORE/X Job to {}".format(uuidcode, url+"/jobs"))
            text, status_code, response_header = unicore_communication.request(app.log,
                                                                               uuidcode,
                                                                               method,
                                                                               method_args)
            if status_code != 201:
                app.log.warning("{} - Could not submit Job. Response from UNICORE/X: {} {} {}.".format(uuidcode, text, status_code, remove_secret(response_header)))
                raise Exception("{} - Could not submit Job. Throw exception because of wrong status_code: {}".format(uuidcode, status_code))
            else:
                unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                kernelurl = response_header['Location']
        except:
            app.log.exception("{} - Could not submit Job. {} {}".format(uuidcode, method, remove_secret(method_args)))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)
            return "", 539

        # get properties of job
        for i in range(5):  # @UnusedVariable        
            properties_json = {}
            try:
                method = "GET"
                method_args = {"url": kernelurl,
                               "headers": unicore_header,
                               "certificate": cert}
                app.log.info("{} - Get Properties of UNICORE/X Job {}".format(uuidcode, kernelurl))
                text, status_code, response_header = unicore_communication.request(app.log,
                                                                                   uuidcode,
                                                                                   method,
                                                                                   method_args)
                if status_code != 200:
                    if i < 4:
                        app.log.debug("{} - Could not get properties of Job. Try again in 2 seconds".format(uuidcode))
                        sleep(2)
                    else:
                        app.log.warning("{} - Could not get properties of Job. Response from UNICORE/X: {} {} {}.".format(uuidcode, text, status_code, remove_secret(response_header)))
                        raise Exception("{} - Could not get properties of Job. Throw exception because of wrong status_code: {}".format(uuidcode, status_code))
                else:
                    unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                    properties_json = json.loads(text)
                    if properties_json.get('status') == 'UNDEFINED' and i < 4:
                        app.log.debug("{} - Received status UNDEFINED. Try again in 2 seconds".format(uuidcode))
                        sleep(2)
                    else:
                        break
            except:
                app.log.exception("{} - Could not get properties of Job. {} {}".format(uuidcode, method, remove_secret(method_args)))
                app.log.trace("{} - Call stop_job".format(uuidcode))
                stop_job(app.log,
                         uuidcode,
                         servername,
                         request.headers,
                         app.urls)
                return "", 539


        # get file directory
        # this will be used in get. Ask it here once and send it to get() afterwards
        filedirectory = ""
        try:
            method = "GET"
            method_args = {"url": properties_json['_links']['workingDirectory']['href'],
                           "headers": unicore_header,
                           "certificate": cert}
            app.log.info("{} - Get path of file directory of UNICORE/X Job".format(uuidcode))
            text, status_code, response_header = unicore_communication.request(app.log,
                                                                               uuidcode,
                                                                               method,
                                                                               method_args)
            if status_code != 200:
                app.log.warning("{} - Could not get filedirectory. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
                raise Exception("{} - Could not get filedirectory. Throw exception because of wrong status_code: {}".format(uuidcode, status_code))
            else:
                unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
                filedirectory = json.loads(text)['_links']['files']['href']
        except:
            app.log.exception("{} - Could not get filedirectory. {} {}".format(uuidcode, method, remove_secret(method_args)))
            app.log.trace("{} - Call stop_job".format(uuidcode))
            stop_job(app.log,
                     uuidcode,
                     servername,
                     request.headers,
                     app.urls)
            return "", 539

        return "", 201, {'kernelurl': kernelurl,
                         'filedir': filedirectory,
                         'X-UNICORE-SecuritySession': unicore_header.get('X-UNICORE-SecuritySession')}


    def delete(self):
        # Track actions through different webservices.
        uuidcode = request.headers.get('uuidcode', '<no uuidcode>')
        app.log.info("{} - Delete Server".format(uuidcode))
        app.log.trace("{} - Headers: {}".format(uuidcode, request.headers.to_list()))

        # Check for the J4J intern token
        validate_auth(app.log,
                      uuidcode,
                      request.headers.get('Intern-Authorization', None))

        accesstoken, expire, security_session = stop_job(app.log,
                                                         uuidcode,
                                                         request.headers.get('servername'),
                                                         request.headers,
                                                         app.urls,
                                                         False)
        app.log.trace("{} - Return: {};{};{}".format(uuidcode, accesstoken, expire, security_session))

        return "", 200, {'accesstoken': accesstoken,
                         'expire': str(expire),
                         'X-UNICORE-SecuritySession': security_session}
