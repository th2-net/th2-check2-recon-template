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

from th2_check2_recon.recon import Recon
from th2_check2_recon.services import MessageComparator
from th2_common.schema.factory.common_factory import CommonFactory
from th2_grpc_util.message_comparator_service import MessageComparatorService


logger = logging.getLogger(__name__)


factory = CommonFactory.create_from_kubernetes()
event_router = factory.event_batch_router
message_router = factory.message_parsed_batch_router
custom_config = factory.create_custom_configuration()
grpc_router = factory.grpc_router
message_comparator = MessageComparator(grpc_router.get_service(MessageComparatorService))
grpc_server = grpc_router.start_server()

recon = Recon(event_router, message_router, custom_config, message_comparator, grpc_server)


def receive_signal(signum, frame):
    logger.info('SIGTERM received')
    try:
        recon.stop()
    finally:
        factory.close()


signal.signal(signal.SIGTERM, receive_signal)

try:
    recon.start()
except KeyboardInterrupt or Exception:
    logger.exception('Unknown error. Recon won\'t be stopped')
