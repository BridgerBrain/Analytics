FROM python:3.9.12-slim-buster

MAINTAINER Shahar Levin

COPY requirements.txt /tmp/

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

RUN adduser appuser; mkdir -p /usr/src/app ; chown appuser:appuser -R /usr/src/app
WORKDIR /usr/src/app
COPY --chown=appuser:appuser app /usr/src/app

USER appuser
CMD [ "python3", "main.py"]
