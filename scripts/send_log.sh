#!/usr/bin/env python3 
import socket, sys, struct

with open(sys.argv[1], 'rb') as f:
    data_to_send = f.read()



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

HOST = 'localhost'
for port in port_list:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, port))
    s.send(struct.pack('>L', len(data_to_send)))
    s.send(data_to_send)
    s.close()
