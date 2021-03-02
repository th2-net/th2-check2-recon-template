# Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

from th2_check2_recon.recon import Recon
from th2_common.schema.factory.common_factory import CommonFactory

logging.config.fileConfig(fname=str(sys.argv[-1]), disable_existing_loggers=False)
logger = logging.getLogger()

factory = CommonFactory()
grpc_router = factory.grpc_router
message_router = factory.message_parsed_batch_router
custom_config = factory.create_custom_configuration()
event_router = factory.event_batch_router

recon = Recon(event_router, grpc_router, message_router, custom_config)


def receive_signal():
    recon.stop()
    factory.close()


signal.signal(signal.SIGTERM, receive_signal)

try:
    recon.start()
except KeyboardInterrupt or Exception:
    logger.exception("Except error and try stop Recon")
finally:
    recon.stop()
