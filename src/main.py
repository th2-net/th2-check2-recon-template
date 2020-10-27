# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging.config
import os
import signal
import sys

import yaml
from th2recon.recon import Recon

recon_config_path = str(sys.argv[1])
log_file_path = str(sys.argv[2])

logging.config.fileConfig(fname=log_file_path, disable_existing_loggers=False)
logger = logging.getLogger()

RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY = os.getenv('RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY')

with open(recon_config_path, 'r') as file:
    recon_config: dict = yaml.load(file, Loader=yaml.FullLoader)

recon_config['rabbit'].update({'host': RABBITMQ_HOST,
                               'port': RABBITMQ_PORT,
                               'vhost': RABBITMQ_VHOST,
                               'user': RABBITMQ_USER,
                               'password': RABBITMQ_PASS})
recon_config['mq'].update({'exchange_name': RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY})

recon = Recon(recon_config)


def receive_signal():
    recon.stop()


signal.signal(signal.SIGTERM, receive_signal)

try:
    recon.start()
except KeyboardInterrupt or Exception:
    logger.exception("Except error and try stop Recon")
finally:
    recon.stop()
