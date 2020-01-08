'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import requests

from contextlib import closing
from random import randint

from app.utils_file_loads import get_j4j_tunnel_token


def get_remote_node(app_logger, uuidcode, tunnel_url_remote, nodelist):
    app_logger.debug("{} - Get remote node".format(uuidcode))
    header = {'Intern-Authorization': get_j4j_tunnel_token(),
              'uuidcode': uuidcode}
    while len(nodelist) > 0:
        i = randint(0, len(nodelist)-1)
        try:
            app_logger.info("{} - Get J4J_Tunnel {}".format(uuidcode, tunnel_url_remote))
            with closing(requests.get(tunnel_url_remote,
                                      params = { 'node': nodelist[i] },
                                      headers = header,
                                      timeout = 60)) as r:
                if r.status_code == 217:
                    app_logger.trace("{} - Use {} as remote node".format(uuidcode, nodelist[i]))
                    return nodelist[i]
                elif r.status_code == 218:
                    try:
                        j4j_start_remote_tunnel(app_logger,
                                                uuidcode,
                                                tunnel_url_remote,
                                                nodelist[i],
                                                header)
                        app_logger.debug("{} - Start remote tunnel for {}".format(uuidcode, nodelist[i]))
                        app_logger.trace("{} - Use {} as remote node".format(uuidcode, nodelist[i]))
                        return nodelist[i]
                    except Exception as e:
                        app_logger.warning("{} - Could not start remote tunnel for {}: {}".format(uuidcode, nodelist[i], str(e)))
                        del nodelist[i]
                else:
                    app_logger.warning("{} - Unexpected status_code ({}) for node: {}. Try another node.".format(uuidcode, r.status_code, nodelist[i]))
                    del nodelist[i]
        except:
            app_logger.exception("{} - Could not get remote tunnelnode".format(uuidcode))
            del nodelist[i]
    app_logger.warning("{} - All nodes failed. Cannot start service".format(uuidcode))
    raise Exception('{} - No Remote Tunnel active'.format(uuidcode))



def j4j_start_remote_tunnel(app_logger, uuidcode, tunnel_url_remote, node, h):
    app_logger.info("{} - Post J4J_Tunnel remote {}".format(uuidcode, tunnel_url_remote))
    with closing(requests.post(tunnel_url_remote,
                               headers = h,
                               json = {'node': node},
                               timeout = 60)) as r:
        if r.status_code == 217:
            return
        raise Exception("{} - Could not start remote tunnel: {} {}".format(uuidcode, r.text, r.status_code))

def j4j_start_tunnel(app_logger, uuidcode, tunnel_url, h, data):
    app_logger.info("{} - Post J4J_Tunnel {}".format(uuidcode, tunnel_url))
    with closing(requests.post(tunnel_url,
                               headers = h,
                               json = data,
                               timeout = 60)) as r:
        if r.status_code == 201:
            return
        raise Exception("{} - Could not start tunnel: {} {}".format(uuidcode, r.text, r.status_code))

def close(app_logger, uuidcode, hub_tunnel_url, tunnel_info):
    app_logger.debug("{} - Try to close tunnel.".format(uuidcode))
    tunnel_header = {
        "Intern-Authorization": get_j4j_tunnel_token(),
        "Content-Type": "application/json",
        "uuidcode": uuidcode
        }
    app_logger.info("{} - Delete J4J_Tunnel {}".format(uuidcode, hub_tunnel_url))
    with closing(requests.delete(hub_tunnel_url,
                                 params = tunnel_info,
                                 headers = tunnel_header,
                                 timeout = 60)) as r:
        if r.status_code == 204 or r.status_code == 200:
            return
        raise Exception("{} - Could not stop tunnel: {} {}".format(uuidcode, r.text, r.status_code))
