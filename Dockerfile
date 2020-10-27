FROM python:3.8-slim

WORKDIR /usr/src/app

ARG USERNAME
ARG PASSWORD

COPY . .

ENV TH2_CORE_VERSION='2.0.16' \
    RABBITMQ_HOST=some-host-name-or-ip \
    RABBITMQ_PORT=7777 \
    RABBITMQ_VHOST=someVhost \
    RABBITMQ_USER=some_user \
    RABBITMQ_PASS=some_pass \
    RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=some_exchange;

RUN pip install th2-recon==$TH2_CORE_VERSION -i https://$USERNAME:$PASSWORD@nexus.exactpro.com/repository/th2-pypi/simple/ --extra-index-url https://pypi.python.org/simple/

CMD [ "python", "./src/main.py", "config.yml", "log_config.conf"]