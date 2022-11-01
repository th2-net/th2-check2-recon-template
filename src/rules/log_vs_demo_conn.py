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

import logging
from typing import List

from th2_common_utils import event_utils

from th2_check2_recon import rule
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupDescription, ReconMessageUtils
from th2_grpc_common.common_pb2 import Event

logger = logging.getLogger(__name__)


class Rule(rule.Rule):

    def get_name(self) -> str:
        return "log vs demo-conn"

    def get_description(self) -> str:
        return "NewOrderSingle message written to the logs and original " \
               "NewOrderSingle message sent through the conn are the same"

    def get_attributes(self) -> List[str]:
        return ['parsed', 'subscribe']

    def description_of_groups(self) -> dict:
        return {
            'NOS_LOG': MessageGroupDescription(single=True),
            'NOS_CONN': MessageGroupDescription(single=True)
        }

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = ReconMessageUtils.get_message_type(message)
        session_alias = ReconMessageUtils.get_session_alias(message)
        if session_alias not in ['demo-conn1', 'demo-conn2', 'demo_log.txt'] or \
                message_type not in ['NewOrderSingle']:
            return

        if message_type == 'NewOrderSingle' and ReconMessageUtils.get_value(message, 'SecondaryClOrdID') == "":
            logger.info(f"RULE '{self.get_name()}'. NOS with empty SecondaryClOrdID: {message.proto_message}.")
            return

        if session_alias in ['demo-conn1', 'demo-conn2']:
            message.group_name = 'NOS_CONN'
        elif session_alias in ['demo_log.txt']:
            message.group_name = 'NOS_LOG'

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        cl_ord_id = ReconMessageUtils.get_value(message, 'SecondaryClOrdID')
        security_id = ReconMessageUtils.get_value(message, 'SecurityID')
        hash_string = ''
        for value in [cl_ord_id, security_id]:
            if value == '':
                return
            hash_string += value
        message.hash = hash(hash_string)
        message.hash_info['SecondaryClOrdID'] = cl_ord_id
        message.hash_info['SecurityID'] = security_id

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: input_messages: {messages}")

        ignore_fields = ['CheckSum', 'BodyLength', 'SendingTime', 'TransactTime', 'MsgSeqNum', 'ClOrdID']
        verification_component = self.message_comparator.compare_messages(messages, ignore_fields)

        info_for_name = {}
        for message in messages:
            info_for_name.update(message.hash_info)

        body = event_utils.create_event_body(verification_component)
        attach_ids = [ReconMessageUtils.get_message_id(msg) for msg in messages]
        return event_utils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}'",
                                        status=verification_component.status,
                                        attached_message_ids=attach_ids,
                                        body=body)
