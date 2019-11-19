# J4J_Worker

FROM ubuntu:18.04

RUN apt update && apt install -y ssh=1:7.6p1-4ubuntu0.3 && apt install -y python3=3.6.7-1~18.04 && apt install -y python3-pip=9.0.1-2.3~ubuntu1.18.04.1 && apt install -y net-tools=1.60+git20161116.90da8a0-1ubuntu1 && apt install -y inotify-tools=3.14-2 && DEBIAN_FRONTEND=noninteractive apt install -y tzdata=2019c-0ubuntu0.18.04 && ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

RUN pip3 install flask-restful==0.3.7 uwsgi==2.0.17.1 pyunicore==0.5.7

RUN mkdir -p /etc/j4j/J4J_Worker

RUN adduser --disabled-password --gecos '' worker

RUN chown worker:worker /etc/j4j/J4J_Worker

USER worker

COPY --chown=worker:worker ./app /etc/j4j/J4J_Worker/app

COPY --chown=worker:worker ./app.py /etc/j4j/J4J_Worker/app.py

COPY --chown=worker:worker ./scripts /etc/j4j/J4J_Worker

COPY --chown=worker:worker ./uwsgi.ini /etc/j4j/J4J_Worker/uwsgi.ini

WORKDIR /etc/j4j/J4J_Worker

CMD /etc/j4j/J4J_Worker/start.sh
