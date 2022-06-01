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
from typing import Optional

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils, TableComponent, MessageUtils
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Event, EventStatus, Message


logger = logging.getLogger(__name__)


def latency_check(response_message: Message, request_message: Message):
    return (MessageUtils.get_timestamp_ns(response_message) - MessageUtils.get_timestamp_ns(request_message)) / 1000


class Group:
    REQUEST = 'Request'
    RESPONSE = 'Response'


class Rule(rule.Rule):

    def get_name(self) -> str:
        return 'Latency by Timestamp Rule'

    def get_description(self) -> str:
        return 'Rule for calculating latency between two message streams by Timestamp in their metadata'

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        return {Group.REQUEST: MessageGroupType.single,
                Group.RESPONSE: MessageGroupType.multi}

    def configure(self, configuration: dict):
        self.request_message_types = configuration.get('RequestMessageTypes', ['NewOrderSingle'])
        self.response_message_types = configuration.get('ResponseMessageTypes', ['ExecutionReport'])
        self.hash_field = configuration.get('HashField', 'ClOrdID')

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type

        if message_type in self.request_message_types:
            message.group_id = Group.REQUEST
        elif message_type in self.response_message_types:
            message.group_id = Group.RESPONSE

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        hash_field = message.proto_message.fields[self.hash_field].simple_value
        message.hash = hash(hash_field)
        message.hash_info[self.hash_field] = hash_field

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:

        hash_field = messages[0].proto_message.fields[self.hash_field].simple_value

        request_message: Optional[Message] = None
        request_message_type = ''
        response_message: Optional[Message] = None
        response_message_type = ''

        latency_type = 'Unknown'

        message: ReconMessage
        for message in messages:
            proto_message = message.proto_message
            message_type = proto_message.metadata.message_type
            if message_type in self.request_message_types:
                request_message = proto_message
                request_message_type = message_type
            elif message_type in self.response_message_types:
                response_message = proto_message
                response_message_type = message_type

        if response_message_type == 'ExecutionReport':
            exec_type = response_message.fields['ExecType'].simple_value
            ord_status = response_message.fields['OrdStatus'].simple_value

            if request_message_type == 'NewOrderSingle':
                if exec_type == 'A' and ord_status == 'A':
                    latency_type = 'PendingNew'
                elif exec_type == '0' and ord_status == '0':
                    latency_type = 'New'
                elif exec_type == 'F' and ord_status in ['1', '2'] and \
                        request_message.fields['LastLiquidityInd'].simple_value == '2':
                    latency_type = 'Trade'
                elif exec_type == '8' and ord_status == '8':
                    latency_type = 'NewReject'

            elif request_message_type == 'OrderCancelRequest':
                if exec_type == '6' and ord_status == '6':
                    latency_type = 'PendingCancel'
                elif (exec_type == '4' and ord_status == '4') or (exec_type == 'C' and ord_status == 'C'):
                    latency_type = 'Cancel'

            elif request_message_type == 'OrderCancelReplaceRequest':
                if exec_type == 'E' and ord_status == 'E':
                    latency_type = 'PendingReplace'
                elif exec_type == '5' and ord_status in ['0', '1']:
                    latency_type = 'Replace'

        elif response_message_type == 'OrderCancelReject':
            if request_message_type == 'OrderCancelReplaceRequest':
                latency_type = 'ReplaceReject'

            elif request_message_type == 'OrderCancelRequest':
                latency_type = 'CancelReject'

        latency = latency_check(response_message, request_message)

        table = TableComponent(['Name', 'Value'])
        table.add_row('Request Message Type', request_message_type)
        table.add_row('Response Message Type', response_message_type)
        table.add_row(f'{self.hash_field}', hash_field)
        table.add_row('Latency type', latency_type)
        table.add_row('Latency', latency)

        logger.debug('Rule: %s. Latency with type %s between %s and %s message with %s = %s is equal to %s',
                     self.get_name(), latency_type, request_message_type, response_message_type,
                     self.hash_field, hash_field, latency)

        body = EventUtils.create_event_body(table)

        attach_ids = [msg.proto_message.metadata.id for msg in messages]

        return EventUtils.create_event(name=f"Latency between messages with {self.hash_field}: '{hash_field}'",
                                       status=EventStatus.SUCCESS,
                                       attached_message_ids=attach_ids,
                                       body=body)
