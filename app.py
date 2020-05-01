'''
Created on May 14, 2019

@author: Tim Kreuzer
@mail: t.kreuzer@fz-juelich.de
'''

import logging.config
import socket
import random
import json
import os
import requests

from contextlib import closing
from time import sleep
from logging.handlers import SMTPHandler
from flask import Flask
from flask_restful import Api

from app.jobs import Jobs
from app.health import HealthHandler
from app.dashboards import Dashboards

with open('/etc/j4j/j4j_mount/j4j_common/mail_receiver.json') as f:
    mail = json.load(f)

logger = logging.getLogger('J4J_UNICORE')
logging.addLevelName(9, "TRACE")
def trace_func(self, message, *args, **kws):
    if self.isEnabledFor(9):
        # Yes, logger takes its '*args' as 'args'.
        self._log(9, message, args, **kws)
logging.Logger.trace = trace_func
mail_handler = SMTPHandler(
    mailhost='mail.fz-juelich.de',
    fromaddr='jupyter.jsc@fz-juelich.de',
    toaddrs=mail.get('receiver'),
    subject='J4J_UNICORE Error'
)
mail_handler.setLevel(logging.ERROR)
mail_handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(filename)s ( Line=%(lineno)d ): %(message)s'
))

# Override logging.config.file_config, so that the logfilename will be send to the parser, each time the logging.conf will be updated
def j4j_file_config(fname, defaults=None, disable_existing_loggers=True):
    if not defaults:
        defaults={'logfilename': '/etc/j4j/j4j_mount/j4j_unicore/logs/{}_{}_w.log'.format(socket.gethostname(), os.getpid())}
    import configparser

    if isinstance(fname, configparser.RawConfigParser):
        cp = fname
    else:
        cp = configparser.ConfigParser(defaults)
        if hasattr(fname, 'readline'):
            cp.read_file(fname)
        else:
            cp.read(fname)

    formatters = logging.config._create_formatters(cp)

    # critical section
    logging._acquireLock()
    try:
        logging._handlers.clear()
        del logging._handlerList[:]
        # Handlers add themselves to logging._handlers
        handlers = logging.config._install_handlers(cp, formatters)
        logging.config._install_loggers(cp, handlers, disable_existing_loggers)
    finally:
        logging._releaseLock()

logging.config.fileConfig = j4j_file_config
logging.config.fileConfig('/etc/j4j/j4j_mount/j4j_unicore/logging.conf')


num_procs = 1
try:
    with open('/etc/j4j/J4J_UNICORE/uwsgi.ini', 'r') as f:
        uwsgi_ini = f.read()
    num_procs = int(uwsgi_ini.split('processes = ')[1].split('\n')[0])
except:
    num_procs = 1

port_list = []
for i in range(num_procs):
    port_list.append(9990+i)

s = random.randint(0, num_procs*100)
sleep(s/100)
t_logs = None
while True:
    port = random.choice(port_list)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('localhost', port))
        if res == 111:
            t_logs = logging.config.listen(port)
            t_logs.start()
            logger.info("Listen on Port: {}".format(port))
            break
    port_list.remove(port)
    if len(port_list) == 0:
        break
    s = random.randint(0,200)
    sleep(s/100)



class FlaskApp(Flask):
    log = None
    with open('/etc/j4j/j4j_mount/j4j_common/urls.json') as f:
        urls = json.loads(f.read())
    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger('J4J_UNICORE')
        health_url = self.urls.get('tunnel', {}).get('url_health')
        self.log.info("StartUp - Check if ssh service is running")
        while True:
            try:
                with closing(requests.get(health_url, headers={}, verify=False, timeout=5)) as r:
                    if r.status_code == 200:
                        self.log.debug("StartUp - J4J_Tunnel answered with 200")
                        break
            except:
                pass
            self.log.debug("StartUp - Could not reach ssh service. Try again in 5 seconds")
            sleep(5)
        super(FlaskApp, self).__init__(*args, **kwargs)

application = FlaskApp(__name__)
if not application.debug:
    application.log.addHandler(mail_handler)
api = Api(application)

api.add_resource(Jobs, "/jobs")
api.add_resource(Dashboards, "/dashboards")
api.add_resource(HealthHandler, "/health")
if __name__ == "__main__":
    application.run(host='0.0.0.0', port=9006)
    logging.config.stopListening()
    t_logs.join()
