FROM python:3.8-slim

WORKDIR /usr/src/app

ARG USERNAME
ARG PASSWORD

COPY . .

ENV TH2_CORE_VERSION='1.1.68' \
    RABBITMQ_HOST=some-host-name-or-ip \
    RABBITMQ_PORT=7777 \
    RABBITMQ_VHOST=someVhost \
    RABBITMQ_USER=some_user \
    RABBITMQ_PASS=some_pass \
    RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=some_exchange \
    ROUTING_KEYS='{"mq_key1", "mq_key2"}' \
    EVENT_STORAGE=event-store-host-name-or-ip:9999 \
    COMPARATOR_URI=utility-host-name-or-ip:9999 \
    CACHE_SIZE=10 \
    BUFFER_SIZE=1000 \
    RECON_TIMEOUT=5 \
    TIME_INTERVAL=10 \
    RULES_CONFIGURATIONS_FILE=../rules_configurations.yaml \
    EVENT_BATCH_MAX_SIZE=32 \
    EVENT_BATCH_SEND_INTERVAL=1;

RUN pip install th2-recon==$TH2_CORE_VERSION -i https://$USERNAME:$PASSWORD@nexus.exactpro.com/repository/th2-pypi/simple/ --extra-index-url https://pypi.python.org/simple/

CMD [ "python", "./src/main.py", "log_config.conf"]