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
import signal
import sys

import yaml
from th2recon.recon import Recon

recon_config_path = str(sys.argv[1])
log_file_path = str(sys.argv[2])

logging.config.fileConfig(fname=log_file_path, disable_existing_loggers=False)
logger = logging.getLogger()

with open(recon_config_path, 'r') as file:
    recon_config = yaml.load(file, Loader=yaml.FullLoader)

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
