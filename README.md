## Introduction
This component allows you to compare two message streams and publish the comparison result to the report. 
Comparison is made using rules, the interface of which is implemented by the user for various tasks.
The two main methods for comparing messages are `hash(message)` and `check(message_a, message_b)`.
Method `hash()` computes a hash string from the received message and returns it. 
Method `check()` makes a more detailed comparison of received messages and returns `Event` of comparison.

## Installing
Create a `pip.conf` file in the root of the virtual environment directory with the following content:
```
[global]
index = https://nexus.exactpro.com/repository/th2-pypi/pypi
index-url = https://nexus.exactpro.com/repository/th2-pypi/simple
extra-index-url= https://pypi.org/simple
```
Use the following command to install the required dependencies:
```
pip install -r requirements.txt
```

## Example of Recon component env variables:
```
RABBITMQ_HOST=some-host-name-or-ip
RABBITMQ_PORT=7777
RABBITMQ_VHOST=someVhost
RABBITMQ_USER=some_user
RABBITMQ_PASS=some_pass
RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY=some_exchange
ROUTING_KEYS='{"mq_key1", "mq_key2"}' - format will be changed to JSON '["mq_key1", "mq_key2"]'
EVENT_STORAGE=event-store-host-name-or-ip:9999 - it will be split to TH2_EVENT_STORAGE_GRPC_HOST, TH2_EVENT_STORAGE_GRPC_PORT
COMPARATOR_URI=utility-host-name-or-ip:9999 - it will be split to TH2_UTILITY_GRPC_HOST, TH2_UTILITY_GRPC_PORT
CACHE_SIZE=10 - Max number of messages in cache for every queue
BUFFER_SIZE=1000 - Max number of messages in buffer for every queue
RECON_TIMEOUT=5 - Timeout in seconds to check incoming messages from queue (need more detail)
TIME_INTERVAL=10 - Window size in seconds for observing messages in cache (rewrite)
RECON_NAME=DEMO - Name Recon report after symbol '_'. 'Recon_<RECON_NAME>'.
RULES_CONFIGURATIONS_FILE - path to the rule configuration file
EVENT_BATCH_MAX_SIZE=32 - Max number of events per batch
EVENT_BATCH_SEND_INTERVAL=1 - Interval in seconds between of sending batches with events
```
