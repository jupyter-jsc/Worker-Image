'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''


import datetime
import os
import uuid
import json


from app.utils_file_loads import get_nodes, get_jlab_conf, get_inputs,\
    get_hub_port, get_fastnet_changes, get_base_url, get_queue_support
from app.tunnel_communication import get_remote_node
from app.unity_communication import renew_token
from app import unicore_communication
from app.utils import remove_secret

def abort_job(app_logger, uuidcode, kernelurl, unicore_header, cert):
    app_logger.debug("uuidcode={} - Try to abort job with kernelurl: {}".format(uuidcode, kernelurl))
    try:
        # If the API of UNICORE will change, the additional GET call might be necessary.
        # Since the action:abort url is (right now) always: kernelurl + /actions/abort we will just use this
        """
        method = "GET"
        method_args = { "url": kernelurl, "headers": unicore_header, "certificate", cert }
        text, status_code, response_header = unicore_communication.request(app_logger,
                                                                           uuidcode,
                                                                           method,
                                                                           method_args)
        if status_code != 200
            ...
        else:
            url = json.loads(text)['_links']['action:abort']['href']
        """
        method = "POST"
        method_args = {"url": kernelurl + '/actions/abort',
                       "headers": unicore_header,
                       "data": "{}",
                       "certificate": cert}

        app_logger.info("uuidcode={} - Abort UNICORE/X Job {}".format(uuidcode, kernelurl))
        text, status_code, response_header = unicore_communication.request(app_logger,
                                                                           uuidcode,
                                                                           method,
                                                                           method_args)

        if status_code != 200:
            app_logger.warning("uuidcode={} - Could not abort Job. Response from UNICORE/X: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
        else:
            unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
    except:
        app_logger.exception("uuidcode={} - Could not abort Job.".format(uuidcode))


def destroy_job(app_logger, uuidcode, kernelurl, unicore_header, cert):
    app_logger.debug("uuidcode={} - Try to destroy Job with kernelurl: {}".format(uuidcode, kernelurl))
    method = "DELETE"
    method_args = {"url": kernelurl,
                   "headers": unicore_header,
                   "certificate": cert}
    try:
        app_logger.info("uuidcode={} - Destroy UNICORE/X Job".format(uuidcode))
        text, status_code, response_header = unicore_communication.request(app_logger,
                                                                           uuidcode,
                                                                           method,
                                                                           method_args)
        if status_code > 399:
            app_logger.warning("uuidcode={} - Could not destroy job. WorkDirectory may still exist. UNICORE/X Response: {} {} {}".format(uuidcode, text, status_code, remove_secret(response_header)))
    except:
        app_logger.exception("uuidcode={} - Could not destroy job.".format(uuidcode))

# Create Header Dict
def create_header(app_logger, uuidcode, request_headers, app_hub_url_proxy_route, app_hub_url_token, username, servername):
    app_logger.debug("uuidcode={} - Create UNICORE/X Header".format(uuidcode))
    accesstoken, expire = renew_token(app_logger,
                                      uuidcode,
                                      request_headers.get("tokenurl"),
                                      request_headers.get("authorizeurl"),
                                      request_headers.get("refreshtoken"),
                                      request_headers.get('accesstoken'),
                                      request_headers.get('expire'),
                                      request_headers.get('jhubtoken'),
                                      app_hub_url_proxy_route,
                                      app_hub_url_token,
                                      username,
                                      servername)
    unicore_header = {"Accept": "application/json",
                      "User-Agent": request_headers.get("User-Agent", "Jupyter@JSC"),
                      "X-UNICORE-User-Preferences": "uid:{},group:{}".format(request_headers.get('account'), request_headers.get('project')),
                      "Content-Type": "application/json",
                      "Authorization": "Bearer {}".format(accesstoken)}
    if request_headers.get('project') == "default":
        unicore_header["X-UNICORE-User-Preferences"] = "uid:{}".format(request_headers.get('account'))

    if request_headers.get('X-UNICORE-SecuritySession', None):
        unicore_header['X-UNICORE-SecuritySession'] = request_headers.get('X-UNICORE-SecuritySession')
        # "session": orig_header.get("session")
    app_logger.trace("uuidcode={} - UNICORE/X Header: {}".format(uuidcode, unicore_header))
    return unicore_header, accesstoken, expire

#
def create_unicore8_job(app_logger, uuidcode, request_json, project, unicore_input):
    app_logger.debug("uuidcode={} - Create UNICORE/X-8 Job.".format(uuidcode))
    env_list = []
    for key, value in request_json.get('Environment', {}).items():
      env_list.append('{}={}'.format(key, value))
    job = {'ApplicationName': 'Bash shell',
           'Environment': env_list,
           'Imports': []}
    queue_support = get_queue_support()
    if request_json.get('system') == "JUROPA":
        job['Project'] = 'root'
    for inp in unicore_input:
        job['Imports'].append(
            {
                "From": "inline://dummy",
                "To"  : inp.get('To'),
                "Data": inp.get('Data'),
            }
        )
    if request_json.get('partition') == 'LoginNode':
        job['Executable'] = '/bin/bash'
        job['Arguments'] = ['.start.sh']
        job['Job type'] = 'interactive'
        app_logger.trace("uuidcode={} - UNICORE/X Job: {}".format(uuidcode, job))
        return job
    if request_json.get('system').upper() in queue_support.get('supported', []):
        job['Resources'] = { 'Queue': request_json.get('partition')}
    else:
        job['Resources'] = {}
    if request_json.get('reservation', None):
        job['Resources']['Reservation'] = request_json.get('reservation')
    for key, value in request_json.get('Resources').items():
        job['Resources'][key] = value
    job['Executable'] = '/bin/bash'
    job['Arguments'] = ['.start.sh']
    app_logger.trace("uuidcode={} - UNICORE/X Job: {}".format(uuidcode, job))
    return job

# Create Job Dict
def create_job(app_logger, uuidcode, request_json, project, unicore_input):
    app_logger.debug("uuidcode={} - Create UNICORE/X-7 Job.".format(uuidcode))
    job = {'ApplicationName': 'Jupyter4JSC',
           'Environment': request_json.get('Environment', {}),
           'Imports': []}

    queue_support = get_queue_support()

    for inp in unicore_input:
        job['Imports'].append(
            {
                "From": "inline://dummy",
                "To"  : inp.get('To'),
                "Data": inp.get('Data'),
            }
        )

    if request_json.get('partition') == 'LoginNode':
        job['Environment']['UC_PREFER_INTERACTIVE_EXECUTION'] = 'true'
        job['Executable'] = 'bash .start.sh'
        app_logger.trace("uuidcode={} - UNICORE/X Job: {}".format(uuidcode, job))
        return job
    if request_json.get('system').upper() in queue_support.get('supported', []):
        job['Resources'] = { 'Queue': request_json.get('partition')}
    else:
        job['Resources'] = {}
    if request_json.get('reservation', None):
        if len(request_json.get('reservation', '')) > 0 and request_json.get('reservation', 'none').lower() != 'none':
            job['Resources']['Reservation'] = request_json.get('reservation')
    for key, value in request_json.get('Resources').items():
        job['Resources'][key] = value
    job['Executable'] = '.start.sh'
    app_logger.debug("uuidcode={} - UNICORE/X Job: {}".format(uuidcode, job))
    return job

# Create Inputs files
def create_inputs(app_logger, uuidcode, request_json, tunnel_url_remote):
    app_logger.debug("uuidcode={} - Create Inputs for UNICORE/X.".format(uuidcode))
    inp = []
    nodes = get_nodes()
    baseconf = get_jlab_conf()
    inps = get_inputs()
    node = get_remote_node(app_logger,
                           uuidcode,
                           tunnel_url_remote,
                           nodes.get(request_json.get('system').upper()))

    inp.append({ 'To': '.start.sh', 'Data': start_sh(app_logger,
                                                     uuidcode,
                                                     request_json.get('system'),
                                                     request_json.get('Checkboxes'),
                                                     inps) })
    inp.append({ 'To': '.config.py', 'Data': get_config(app_logger,
                                                        uuidcode,
                                                        baseconf,
                                                        request_json.get('port'),
                                                        node,
                                                        request_json.get('Environment', {}).get('JUPYTERHUB_USER')) })
    inp.append({ 'To': '.jbashrc', 'Data': jbashrc(app_logger,
                                                   uuidcode,
                                                   request_json.get('system'),
                                                   inps) })
    inp.append({ 'To': '.jupyter.token', 'Data': request_json.get('Environment').get('JUPYTERHUB_API_TOKEN') })
    try:
        del request_json['Environment']['JUPYTERHUB_API_TOKEN']
        del request_json['Environment']['JPY_API_TOKEN']
    except KeyError:
        pass
    app_logger.trace("uuidcode={} - Inputs for UNICORE/X: {}".format(uuidcode, inp))
    return inp


def get_config(app_logger, uuidcode, baseconf, port, hubapiurlnode, user):
    app_logger.debug("uuidcode={} - Generate config".format(uuidcode))
    hubport = get_hub_port()
    ret = baseconf + '\nc.SingleUserLabApp.port = {}'.format(port)
    hubnode = get_fastnet_changes(hubapiurlnode)
    base_url = get_base_url()
    ret += '\nc.SingleUserLabApp.hub_api_url = "http://{}:{}{}hub/api"'.format(hubnode, hubport, base_url)
    ret += '\nc.SingleUserLabApp.hub_activity_url = "http://{}:{}{}hub/api/users/{}/activity"\n'.format(hubnode, hubport, base_url, user)
    app_logger.trace("uuidcode={} - Config: {}".format(uuidcode, ret.replace("\n","/n")))
    return ret



def copy_log(app_logger, uuidcode, unicore_header, filedir, kernelurl, cert):
    app_logger.debug("uuidcode={} - Copy Log from {}".format(uuidcode, kernelurl))
    # in this directory we will write the complete log from the started server.
    directory = '/etc/j4j/j4j_mount/jobs/{}_{}'.format(kernelurl.split('/')[-1], datetime.datetime.today().strftime('%Y_%m_%d-%H_%M_%S'))
    for i in range(10):
        if os.path.exists(directory):
            add_uuid = uuid.uuid4().hex
            directory = directory + '_' + add_uuid
        if not os.path.exists(directory):
            os.makedirs(directory)
            break
        if i == 9:
            app_logger.warning("uuidcode={} - Could not find a directory to save files".format(uuidcode))
            return
    app_logger.debug("uuidcode={} - Copy Log to {}".format(uuidcode, directory))
    # Get children list
    try:
        app_logger.info("uuidcode={} - Get list of files of UNICORE/X Job".format(uuidcode))
        text, status_code, response_header = unicore_communication.request(app_logger,
                                                                           uuidcode,
                                                                           "GET",
                                                                           {"url": filedir, "headers": unicore_header, "certificate": cert})
        if status_code != 200:
            app_logger.warning("uuidcode={} - Could not save files from {}. Response from UNICORE: {} {} {}".format(uuidcode, kernelurl, text, status_code, remove_secret(response_header)))
            return
        # in UNICORE 8 the answer is a bit different
        children_json = json.loads(text)
        if 'children' in children_json.keys():
            children = json.loads(text).get('children', [])
        elif 'content' in children_json.keys():
            children = list(json.loads(text).get('content', {}).keys())
        else:
            app_logger.warning("uuidcode={} - Could not find any childrens in {}".format(uuidcode, text))
            children = []
        unicore_header['X-UNICORE-SecuritySession'] = response_header['X-UNICORE-SecuritySession']
    except:
        app_logger.exception("uuidcode={} - Could not save files from {}".format(uuidcode, kernelurl))
        return

    # For the file input we need another Accept in the header, save the old one
    hostname = ""
    accept = unicore_header.get('Accept', False)
    unicore_header['Accept'] = 'application/octet-stream'
    app_logger.info("uuidcode={} - Save files in directory {}".format(uuidcode, directory))
    for child in children:
        try:
            content, status_code, response_header = unicore_communication.request(app_logger,
                                                                                  uuidcode,
                                                                                  "GET",
                                                                                  {"url": filedir+'/'+child,
                                                                                   "headers": unicore_header,
                                                                                   "certificate": cert,
                                                                                   "return_content": True})
            if status_code != 200:
                app_logger.warning("uuidcode={} - Could not save file {} from {}. Try next. Response from UNICORE: {} {} {}".format(uuidcode, child, kernelurl, content, status_code, remove_secret(response_header)))
                continue
            with open(directory+'/'+child, 'w') as f:
                f.write(content)
            if child == ".host" or child == "/.host":
                hostname = content.strip()
        except:
            app_logger.exception("uuidcode={} - Could not save file {} from {}".format(uuidcode, child, kernelurl))
            break
    if accept:
        unicore_header['Accept'] = accept
    else:
        del unicore_header['Accept']
    app_logger.debug("uuidcode={} - Log from {} to {} copied".format(uuidcode, kernelurl, directory))
    return hostname


def start_sh(app_logger, uuidcode, system, checkboxes, inputs):
    app_logger.debug("uuidcode={} - Create start.sh file".format(uuidcode))
    startjupyter = '#!/bin/bash\n_term() {\n  echo \"Caught SIGTERM signal!\"\n  kill -TERM \"$child\" 2>/dev/null\n}\ntrap _term SIGTERM\n'
    startjupyter += 'hostname>.host;\n'
    startjupyter += inputs.get(system.upper()).get('start').get('defaultmodules')+'\n'
    startjupyter += 'export JUPYTERHUB_API_TOKEN=`cat .jupyter.token`\n'
    startjupyter += 'export JPY_API_TOKEN=`cat .jupyter.token`\n'
    for scriptpath in checkboxes:
        with open(scriptpath, 'r') as f:
            script = f.read()
        startjupyter += script+'\n'
    if 'executable' in inputs.get(system.upper()).get('start').keys():
        startjupyter += inputs.get(system.upper()).get('start').get('executable')
    else:
        startjupyter += 'jupyter labhub $@ --config .config.py &'
    startjupyter += '\nchild=$!\nwait "$child"'
    startjupyter += '\necho "end">.end\n'
    app_logger.trace("uuidcode={} - start.sh file: {}".format(uuidcode, startjupyter.replace("\n", "/n")))
    return startjupyter

def jbashrc(app_logger, uuidcode, system, inputs):
    app_logger.debug("uuidcode={} - Create jbashrc file".format(uuidcode))
    jbashrc = '#!/bin/bash\n'
    jbashrc += inputs.get(system.upper()).get('jbashrc').get('defaultmodules')+'\n'
    app_logger.trace("uuidcode={} - jbashrc file: {}".format(uuidcode, jbashrc.replace("\n", "/n")))
    return jbashrc

