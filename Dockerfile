FROM python:3.8-slim

WORKDIR /usr/src/app

ARG USERNAME
ARG PASSWORD

COPY . .

ENV TH2_CORE_VERSION='2.1.0-schema-v2-2'

RUN pip install th2-check2-recon==$TH2_CORE_VERSION -i https://$USERNAME:$PASSWORD@nexus.exactpro.com/repository/th2-pypi/simple/ --extra-index-url https://pypi.python.org/simple/

CMD [ "python", "./src/main.py", "log_config.conf"]