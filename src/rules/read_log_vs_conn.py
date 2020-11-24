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

import logging
import string

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils, VerificationComponent, ComparatorUtils
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Direction, Event, EventStatus
from th2_grpc_util.util_pb2 import ComparisonSettings, ComparisonEntryStatus

logger = logging.getLogger()


class Rule(rule.Rule):

    def get_name(self) -> str:
        return "log vs conn"

    def get_description(self) -> str:
        return "NOS from log reconciled with NOS from demo-conn1 and demo-conn2 by ClOrdID"

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        return {'NOS_LOG': MessageGroupType.single,
                'NOS_CONN': MessageGroupType.single}

    def group(self, message: ReconMessage, attributes: tuple):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        direction = message.proto_message.metadata.id.direction
        if session_alias not in ['demo-conn1', 'demo-conn2', 'demo_log.txt'] or \
                message_type not in ['NewOrderSingle']:
            return

        if message_type == 'NewOrderSingle' and \
                message.proto_message.fields['SecondaryClOrdID'].simple_value == "":
            logger.info(f"RULE '{self.get_name()}'. NOS with empty SecondaryClOrdID: {message.proto_message}.")
            return

        if session_alias in ['demo-conn1', 'demo-conn2']:
            message.group_id = 'NOS_CONN'
        elif session_alias in ['demo_log.txt']:
            message.group_id = 'NOS_LOG'

    def hash(self, message: ReconMessage, attributes: tuple):
        cl_ord_id = message.proto_message.fields['SecondaryClOrdID'].simple_value
        security_id = message.proto_message.fields['SecurityID'].simple_value
        val = ''
        for field_name in ['SecondaryClOrdID', 'SecurityID']:
            if message.proto_message.fields[field_name].simple_value == '':
                return
            val += message.proto_message.fields[field_name].simple_value
        message.hash = hash(val)
        message.hash_info['SecondaryClOrdID'] = cl_ord_id
        message.hash_info['SecurityID'] = security_id

    def check(self, messages: [ReconMessage]) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: input_messages: {messages}")

        settings = ComparisonSettings()
        settings.ignore_fields.extend(
            ['CheckSum', 'BodyLength', 'SendingTime', 'TransactTime', 'MsgSeqNum', 'ClOrdID'])
        compare_result = self.message_comparator.compare(messages[0].proto_message, messages[1].proto_message, settings)

        verification_component = VerificationComponent(compare_result.comparison_result)

        info_for_name = dict()
        for message in messages:
            info_for_name.update(message.hash_info)

        body = EventUtils.create_event_body(verification_component)
        status = EventStatus.FAILED if ComparatorUtils.get_status_type(
            compare_result.comparison_result) == ComparisonEntryStatus.FAILED else EventStatus.SUCCESS

        attach_ids = [msg.proto_message.metadata.id for msg in messages]
        return EventUtils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}'",
                                       status=status,
                                       attached_message_ids=attach_ids,
                                       body=body)
