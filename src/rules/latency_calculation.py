# Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
from fnmatch import fnmatch
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

    def __str__(self) -> str:
        if self == LatencyCalculationMode.SENDING_TIME:
            return 'Response SendingTime minus request SendingTime'
        if self == LatencyCalculationMode.RESPONSE_TIME:
            return 'Response Timestamp minus request SendingTime'
        if self == LatencyCalculationMode.CUSTOM:
            return 'Response {} minus request {}'
        else:
            return 'Response Timestamp minus request Timestamp'


class HashInfo:

    def __init__(self, info: dict):
        self.hash_field = info.get('HashField', 'ClOrdID')
        self.is_property = info.get('IsProperty', False)
        self.is_multiple = info.get('IsMultiple', False)


def parse_time(time: str):
    try:
        return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return datetime.strptime(time, '%Y-%m-%dT%H:%M')


def subtract_time(time1: datetime, time2: datetime):
    return (time2 - time1) / timedelta(microseconds=1)


class Rule(rule.Rule):

    def get_name(self) -> str:
        return 'Latency Calculation Rule'

    def get_description(self) -> str:
        return 'Rule for calculating latency between two message streams ' \
               'by different time fields according to chosen mode'

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
        self.request_hash_info = HashInfo(configuration.get('RequestHashInfo', {}))
        self.request_time = configuration.get('RequestTime', 'TransactTime')

        self.response_message_types = configuration.get('ResponseMessageTypes', ['ExecutionReport'])
        self.response_message_session_aliases = configuration.get('ResponseMessageSessionAliases', [])
        self.response_hash_info = HashInfo(configuration.get('ResponseHashInfo', {}))
        self.response_time = configuration.get('ResponseTime', 'TransactTime')

        self.mode = LatencyCalculationMode(configuration.get('Mode', 'Timestamp'))

        self.latency_info = configuration.get('LatencyInfo', 'Latency')
        self.included_properties = configuration.get('Properties', [])

    def determine_message(self, message: ReconMessage):
        message_type: str = message.proto_message['metadata']['message_type']
        session_alias: str = message.proto_message['metadata']['session_alias']

        if message_type in self.request_message_types and \
                (len(self.request_message_session_aliases) == 0 or
                 any(fnmatch(session_alias, wildcard) for wildcard in self.request_message_session_aliases)):
            return Group.REQUEST
        elif message_type in self.response_message_types and \
                (len(self.response_message_session_aliases) == 0 or
                 any(fnmatch(session_alias, wildcard) for wildcard in self.response_message_session_aliases)):
            return Group.RESPONSE

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        group = self.determine_message(message)

        if group == Group.REQUEST:
            message.group_id = Group.REQUEST
        elif group == Group.RESPONSE:
            message.group_id = Group.RESPONSE

    def _set_hash(self, hash_info: HashInfo, message):
        try:
            if hash_info.is_property:
                hash_field = message.proto_message['metadata']['properties'][hash_info.hash_field]
                message.hash_info[hash_info.hash_field] = hash_field
            elif hash_info.is_multiple:
                hash_field = ''
                for field in hash_info.hash_field:
                    field_value = message.proto_message['fields'].get(field)
                    if field_value is not None:
                        hash_field += field_value
                        message.hash_info[field] = field_value
            else:
                hash_field = message.proto_message['fields'][hash_info.hash_field]
                message.hash_info[hash_info.hash_field] = hash_field

            message.hash = hash(hash_field)
        except KeyError:
            if hash_info.hash_field == 'id':
                # Skip such messages
                # We expect that all interest messages should contain ID field.
                logger.debug(
                    "Rule: %s. Skip the message without 'id' field. Message: %s",
                    self.get_name(), message.proto_message)
            else:
                raise

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        group = self.determine_message(message)

        if group == Group.REQUEST:
            self._set_hash(self.request_hash_info, message)
        elif group == Group.RESPONSE:
            self._set_hash(self.response_hash_info, message)

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

        if self.request_hash_info.is_property:
            request_hash_field = request_message['metadata']['properties'][self.request_hash_info.hash_field]
        elif self.request_hash_info.is_multiple:
            request_hash_field = ', '.join(request_message['fields'][field] 
                                           for field in self.request_hash_info.hash_field)
        else:    
            request_hash_field = request_message['fields'][self.request_hash_info.hash_field]
        
        if self.response_hash_info.is_property:
            response_hash_field = response_message['metadata']['properties'][self.response_hash_info.hash_field]
        elif self.response_hash_info.is_multiple:
            response_hash_field = ', '.join(response_message['fields'][field] 
                                            for field in self.response_hash_info.hash_field)
        else:    
            response_hash_field = response_message['fields'][self.response_hash_info.hash_field]

        response_exec_type = response_message['fields'].get('ExecType')
        response_ord_status = response_message['fields'].get('OrdStatus')

        table = TableComponent(['Name', 'Value'])
        table.add_row('Request Message Type', request_message_type)
        table.add_row('Response Message Type', response_message_type)
        table.add_row('Timestamp', str(request_message['metadata']['timestamp']))
        table.add_row('Request match field', str(self.request_hash_info.hash_field))
        table.add_row('Response match field', str(self.response_hash_info.hash_field))
        table.add_row('Match value', request_hash_field)

        if response_exec_type is not None:
            table.add_row('ExecType', response_exec_type)

        if response_ord_status is not None:
            table.add_row('OrdStatus', response_ord_status)

        if self.mode == LatencyCalculationMode.SENDING_TIME:
            request_time = request_message['fields']['header']['SendingTime']
            response_time = response_message['fields']['header']['SendingTime']
            latency = subtract_time(parse_time(request_time), parse_time(response_time))

        elif self.mode == LatencyCalculationMode.RESPONSE_TIME:
            request_time = request_message['fields']['header']['SendingTime']
            response_time = response_message['metadata']['timestamp']

            latency = subtract_time(parse_time(request_time), response_time)

        elif self.mode == LatencyCalculationMode.CUSTOM:

            if self.request_time == 'SendingTime':
                request_time = request_message['fields']['header']['SendingTime']
            else:
                request_time = request_message['fields'][self.request_time]

            if self.response_time == 'SendingTime':
                response_time = response_message['fields']['header']['SendingTime']
            else:
                response_time = response_message['fields'][self.response_time]

            latency = subtract_time(parse_time(request_time), parse_time(response_time))

        else:
            request_time = request_message['metadata']['timestamp']
            response_time = response_message['metadata']['timestamp']
            latency = subtract_time(request_time, response_time)

        table.add_row('Mode', str(self.mode).format(self.response_time, self.request_time))
        table.add_row('Request Time', str(request_time))
        table.add_row('Response Time', str(response_time))
        table.add_row('Latency in us', str(latency))

        logger.debug('Rule: %s. Latency between %s with %s = %s and %s with %s = %s is equal to %s',
                     self.get_name(),
                     request_message_type, str(self.request_hash_info.hash_field), request_hash_field,
                     response_message_type, str(self.response_hash_info.hash_field), response_hash_field,
                     latency)

        body = EventUtils.create_event_body(table)

        attach_ids = [MessageID(connection_id=ConnectionID(session_alias=
                                                           msg.proto_message['metadata']['session_alias']),
                                direction=msg.proto_message['metadata']['direction'],
                                sequence=msg.proto_message['metadata']['sequence'])
                      for msg in messages]

        properties = ', '.join(request_message['metadata']['properties'][key]
                               for key in self.included_properties
                               if key in request_message['metadata']['properties'])

        return EventUtils.create_event(name=f'{self.latency_info} between messages with '
                                            f'{self.request_hash_info.hash_field} = {request_hash_field} and '
                                            f'{self.response_hash_info.hash_field} = {response_hash_field} | {properties}',
                                       status=EventStatus.SUCCESS,
                                       attached_message_ids=attach_ids,
                                       body=body)
