# Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Any, Dict

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils, TableComponent, MessageUtils
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Event, EventStatus, MessageID, ConnectionID

logger = logging.getLogger(__name__)


class Group:
    REQUEST = 'Request'
    RESPONSE = 'Response'


class LatencyCalculationMode(Enum):
    TIMESTAMP = 'Timestamp'
    SENDING_TIME = 'SendingTime'
    RESPONSE_TIME = 'ResponseTime'
    CUSTOM = 'Custom'

    @classmethod
    def _missing_(cls, value: object) -> Any:
        return LatencyCalculationMode.TIMESTAMP


def latency_by_timestamp(response_message: Dict[str, Any], request_message: Dict[str, Any]):
    return (MessageUtils.get_timestamp_ns(response_message) - MessageUtils.get_timestamp_ns(request_message)) / 1000


def latency_by_sending_time(response_message: Dict[str, Any], request_message: Dict[str, Any]):

    request_sending_time = request_message['fields']['header']['SendingTime']
    response_sending_time = response_message['fields']['header']['SendingTime']

    try:
        request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M')

    try:
        response_sending_time = datetime.strptime(response_sending_time, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            response_sending_time = datetime.strptime(response_sending_time, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            response_sending_time = datetime.strptime(response_sending_time, '%Y-%m-%dT%H:%M')

    latency = (response_sending_time - request_sending_time) / timedelta(microseconds=1)
    return latency


def latency_response_time(response_message: Dict[str, Any], request_message: Dict[str, Any]):

    response_timestamp = MessageUtils.get_timestamp_ns(response_message) / 1000

    request_sending_time = request_message['fields']['header']['SendingTime']
    try:
        request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            request_sending_time = datetime.strptime(request_sending_time, '%Y-%m-%dT%H:%M')

    request_sending_time = request_sending_time.timestamp() * 1_000_000

    return response_timestamp - request_sending_time


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
        self.request_message_session_aliases = configuration.get('RequestMessageSessionAliases', [])
        self.request_hash_field = configuration.get('RequestHashField', 'ClOrdID')
        self.request_time = configuration.get('RequestTime', 'TransactTime')

        self.response_message_types = configuration.get('ResponseMessageTypes', ['ExecutionReport'])
        self.response_message_session_aliases = configuration.get('ResponseMessageSessionAliases', [])
        self.response_hash_field = configuration.get('ResponseHashField', 'ClOrdID')
        self.response_time = configuration.get('ResponseTime', 'TransactTime')

        self.mode = LatencyCalculationMode(configuration.get('Mode', 'Timestamp'))

        self.latency_info = configuration.get('LatencyInfo', 'Latency')
        self.included_properties = configuration.get('Properties', [])

    def determine_message(self, message: ReconMessage):
        message_type: str = message.proto_message['metadata']['message_type']
        session_alias: str = message.proto_message['metadata']['session_alias']

        if message_type in self.request_message_types and \
                (len(self.request_message_session_aliases) == 0 or
                 session_alias in self.request_message_session_aliases):
            return Group.REQUEST
        elif message_type in self.response_message_types and \
                (len(self.response_message_session_aliases) == 0 or
                 session_alias in self.response_message_session_aliases):
            return Group.RESPONSE

    def latency_custom(self, response_message: Dict[str, Any], request_message: Dict[str, Any]):

        request_time = request_message['fields'][self.request_time]
        response_time = response_message['fields'][self.response_time]

        try:
            request_time = datetime.strptime(request_time, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                request_time = datetime.strptime(request_time, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                request_time = datetime.strptime(request_time, '%Y-%m-%dT%H:%M')

        try:
            response_time = datetime.strptime(response_time, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                response_time = datetime.strptime(response_time, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                response_time = datetime.strptime(response_time, '%Y-%m-%dT%H:%M')

        latency = (response_time - request_time) / timedelta(microseconds=1)
        return latency

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        group = self.determine_message(message)

        if group == Group.REQUEST:
            message.group_id = Group.REQUEST
        elif group == Group.RESPONSE:
            message.group_id = Group.RESPONSE
            
    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        group = self.determine_message(message)

        if group == Group.REQUEST:
            hash_field = message.proto_message['fields'][self.request_hash_field]
            message.hash = hash(hash_field)
            message.hash_info[self.request_hash_field] = hash_field
        elif group == Group.RESPONSE:
            hash_field = message.proto_message['fields'][self.response_hash_field]
            message.hash = hash(hash_field)
            message.hash_info[self.response_hash_field] = hash_field

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:

        request_message: Optional[Dict[str, Any]] = None
        request_message_type = ''
        response_message: Optional[Dict[str, Any]] = None
        response_message_type = ''

        message: ReconMessage
        for message in messages:
            proto_message = message.proto_message
            group = self.determine_message(message)
            message_type: str = proto_message['metadata']['message_type']

            if group == Group.REQUEST:
                request_message = proto_message
                request_message_type = message_type
            elif group == Group.RESPONSE:
                response_message = proto_message
                response_message_type = message_type

        request_timestamp = str(request_message['metadata']['timestamp'])
        request_hash_field = request_message['fields'][self.request_hash_field]
        response_hash_field = response_message['fields'][self.response_hash_field]

        response_exec_type = response_message['fields'].get('ExecType')
        response_ord_status = response_message['fields'].get('OrdStatus')

        table = TableComponent(['Name', 'Value'])
        table.add_row('Message Type', request_message_type)
        table.add_row('Timestamp', request_timestamp)
        table.add_row(f'{self.request_hash_field}', request_hash_field)

        if response_exec_type is not None:
            table.add_row('ExecType', response_exec_type)

        if response_ord_status is not None:
            table.add_row('OrdStatus', response_ord_status)

        if self.mode == LatencyCalculationMode.SENDING_TIME:
            latency = latency_by_sending_time(response_message, request_message)
        elif self.mode == LatencyCalculationMode.RESPONSE_TIME:
            latency = latency_response_time(response_message, request_message)
        elif self.mode == LatencyCalculationMode.CUSTOM:
            latency = self.latency_custom(response_message, request_message)
            table.add_row(f'{self.request_time}', request_message['fields'][self.request_time])
            table.add_row(f'{self.response_time}', response_message['fields'][self.response_time])
        else:
            latency = latency_by_timestamp(response_message, request_message)

        table.add_row('Latency in us', latency)

        logger.debug('Rule: %s. Latency between %s with %s = %s and %s with %s = %s is equal to %s',
                     self.get_name(),
                     request_message_type, self.request_hash_field, request_hash_field,
                     response_message_type, self.response_hash_field, response_hash_field,
                     latency)

        body = EventUtils.create_event_body(table)

        attach_ids = [MessageID(connection_id=ConnectionID(session_alias=msg.proto_message['metadata']['session_alias']),
                                direction=msg.proto_message['metadata']['direction'],
                                sequence=msg.proto_message['metadata']['sequence'])
                      for msg in messages]

        properties = ', '.join(request_message['metadata']['properties'][key]
                               for key in self.included_properties
                               if key in request_message['metadata']['properties'])

        return EventUtils.create_event(name=f'{self.latency_info} between messages with '
                                            f'{self.request_hash_field} = {request_hash_field} and '
                                            f'{self.response_hash_field} = {response_hash_field} {properties}',
                                       status=EventStatus.SUCCESS,
                                       attached_message_ids=attach_ids,
                                       body=body)
