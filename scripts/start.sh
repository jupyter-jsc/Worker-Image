#!/bin/bash
sed -i -e "s/log_hostname/$HOSTNAME/g" /etc/j4j/J4J_UNICORE/uwsgi.ini
if [ -f "/etc/j4j/J4J_UNICORE/watch_logs.pid" ];then
    kill -9 `cat /etc/j4j/J4J_UNICORE/watch_logs.pid`
    rm /etc/j4j/J4J_UNICORE/watch_logs.pid
fi
/etc/j4j/J4J_UNICORE/watch_logs.sh &
echo $! > /etc/j4j/J4J_UNICORE/watch_logs.pid
uwsgi --ini /etc/j4j/J4J_UNICORE/uwsgi.ini
