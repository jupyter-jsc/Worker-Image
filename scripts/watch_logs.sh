#!/bin/bash

while true; do
  inotifywait -q --event modify --format '%e' /etc/j4j/j4j_mount/j4j_worker/logging.conf
  /etc/j4j/J4J_Worker/send_log.sh /etc/j4j/j4j_mount/j4j_worker/logging.conf 
done
