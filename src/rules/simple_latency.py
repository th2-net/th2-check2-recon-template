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
import threading

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils, TableComponent, MessageUtils, MessageComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Event, EventStatus, Message

logger = logging.getLogger(__name__)


def latency_check(message_response: Message, message_request: Message):
    type1 = message_response.metadata.message_type
    type2 = message_request.metadata.message_type
    latency = (MessageUtils.get_timestamp_ns(message_response) - MessageUtils.get_timestamp_ns(message_request)) / 1000
    return type1, type2, latency


class Group:
    REQUEST = 'Request'
    RESPONSE = 'Response'


class Rule(rule.Rule):

    def get_name(self) -> str:
        return "SimpleLatencyRule"

    def get_description(self) -> str:
        return "SimpleLatencyRule"

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        return {Group.REQUEST: MessageGroupType.single,
                Group.RESPONSE: MessageGroupType.single}

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type

        if message_type == 'NewOrderSingle':
            message.group_id = Group.REQUEST
        elif message_type == 'ExecutionReport':
            message.group_id = Group.RESPONSE

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        cl_ord_id = message.proto_message.fields['ClOrdID'].simple_value
        message.hash = hash(cl_ord_id)
        message.hash_info['ClOrdID'] = cl_ord_id

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        message_types = []
        cl_order_id = messages[0].proto_message.fields['ClOrdID'].simple_value
        latency_type = 'Unknown'
        recv_msg: Message = None
        send_msg: Message = None
        recv_msg_type: str = ''
        send_msg_type: str = ''
        explanation = None

        msg: ReconMessage
        for msg in messages:
            message = msg.proto_message
            message_type = message.metadata.message_type
            message_types.append(message_type)
            if msg.group_id == Group.RESPONSE:
                recv_msg = msg.proto_message
                recv_msg_type = message_type
            else:
                send_msg = msg.proto_message
                send_msg_type = message_type

        if recv_msg_type == 'ExecutionReport':
            exec_type = recv_msg.fields['ExecType'].simple_value
            ord_status = recv_msg.fields['OrdStatus'].simple_value

            logger.info(f"RULE '{self.get_name()}': "
                        f"CHECK: messageER: [ClOrdID:{cl_order_id}, "
                        f"ExecType: {exec_type}, "
                        f"OrdStatus: {ord_status}]")

            if send_msg_type == 'NewOrderSingle':
                if exec_type == 'A' and ord_status == 'A':
                    latency_type = 'PendingNew'
                elif exec_type == '0' and ord_status == '0':
                    latency_type = 'New'
                elif exec_type == 'F' and ord_status in ['1', '2'] and \
                        recv_msg.fields['LastLiquidityInd'].simple_value == '2':
                    latency_type = 'Trade'
                elif exec_type == '8' and ord_status == '8':
                    latency_type = 'NewReject'

            # Should be always in the end.
            if latency_type == 'Unknown':
                explanation = MessageComponent(f"Attention! Unknown messages combination. \n"
                                               f"ER[ExecType]: {exec_type}, ER[OrdStatus]: {ord_status}\n\n"
                                               f" --------------- Recv msg --------------- \n {recv_msg}\n"
                                               f" --------------- Send msg --------------- \n {send_msg}")

        else:
            logger.error(f"RULE '{self.get_name()}': "
                         f"CHECK: Unknown message received. "
                         f"Msg types: {message_types}\n"
                         f"Recv msg: {recv_msg}\n"
                         f"Send msg: {send_msg}")

        type1, type2, latency = latency_check(recv_msg, send_msg)

        table = TableComponent(['Name', 'Value'])
        table.add_row('ClOrdId', cl_order_id)
        table.add_row('Message Response', type1)
        table.add_row('Message Request', type2)
        table.add_row('Latency type', latency_type)
        table.add_row('Latency', latency)
        event_message = f'{type1} and {type2} ' \
                        f'Latency_type: {latency_type} ' \
                        f'Latency = {latency}'
        logger.info(
            f"RULE '{self.get_name()}': Thread: {threading.current_thread().name}: "
            f"EventMessage={event_message}. Latency was calculated for {cl_order_id} between {message_types[0]} "
            f"and {message_types[1]}")

        if explanation is None:
            body = EventUtils.create_event_body(table)
        else:
            body = EventUtils.create_event_body([table, explanation])
        attach_ids = [msg.proto_message.metadata.id for msg in messages]
        status = EventStatus.SUCCESS if latency < self.LATENCY_LIMIT else EventStatus.FAILED
        return EventUtils.create_event(name=f"Match by ClOrdID: '{cl_order_id}'",
                                       status=status,
                                       attached_message_ids=attach_ids,
                                       body=body)
