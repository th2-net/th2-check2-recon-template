# Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
from typing import Dict, Any

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils, TableComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Event, EventStatus, MessageID, ConnectionID

logger = logging.getLogger(__name__)


class LatencyCalculationMode(Enum):
    SENDING_TRANSACT = 'SendingTransact'
    CUSTOM = 'Custom'

    @classmethod
    def _missing_(cls, value: object) -> Any:
        return LatencyCalculationMode.SENDING_TRANSACT

    def __str__(self) -> str:
        if self == LatencyCalculationMode.CUSTOM:
            return '{} minus {}'
        else:
            return 'SendingTime minus TransactTime'


def calculate_latency(time1: str, time2: str):

    try:
        time1 = datetime.strptime(time1, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            time1 = datetime.strptime(time1, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            time1 = datetime.strptime(time1, '%Y-%m-%dT%H:%M')

    try:
        time2 = datetime.strptime(time2, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            time2 = datetime.strptime(time2, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            time2 = datetime.strptime(time2, '%Y-%m-%dT%H:%M')

    latency = (time2 - time1) / timedelta(microseconds=1)
    return latency


class Rule(rule.Rule):

    def get_name(self) -> str:
        return 'Single Message Stream Latency Rule'

    def get_description(self) -> str:
        return 'Rule for calculating latency in single message stream ' \
               'between SendingTime (52) and TransactTime (60) tags'

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        return {'Message': MessageGroupType.single}

    def configure(self, configuration: dict):
        self.message_types = configuration.get('MessageTypes', ['NewOrderSingle'])
        self.session_aliases = configuration.get('SessionAliases', [])
        self.hash_field = configuration.get('HashField', 'ClOrdID')
        self.time1 = configuration.get('Time1', 'TransactTime')
        self.time2 = configuration.get('Time2', 'TransactTime')
        self.mode = LatencyCalculationMode(configuration.get('Mode', 'SendingTransact'))

        self.latency_info = configuration.get('LatencyInfo', 'Latency')
        self.included_properties = configuration.get('Properties', [])

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message['metadata']['message_type']
        session_alias: str = message.proto_message['metadata']['session_alias']

        if message_type in self.message_types and \
                (len(self.session_aliases) == 0 or session_alias in self.session_aliases):
            message.group_id = 'Message'

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        hash_field = message.proto_message['fields'][self.hash_field]
        message.hash = hash(hash_field)
        message.hash_info[self.hash_field] = hash_field

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        message = messages[0]
        proto_message: Dict[str, Any] = message.proto_message
        message_type = proto_message['metadata']['message_type']
        hash_field = proto_message['fields'][self.hash_field]

        table = TableComponent(['Name', 'Value'])
        table.add_row('MessageType', message_type)
        table.add_row('Timestamp', str(proto_message['metadata']['timestamp']))
        table.add_row(f'{self.hash_field}', hash_field)
        table.add_row('Mode', str(self.mode).format(self.time2, self.time1))

        if self.mode == LatencyCalculationMode.CUSTOM:
            time1 = proto_message['fields'][self.time1]
            time2 = proto_message['fields'][self.time2]
        else:
            time1 = proto_message['fields']['TransactTime']
            time2 = proto_message['fields']['header']['SendingTime']

        table.add_row('Time 1', time1)
        table.add_row('Time 2', time2)

        latency = calculate_latency(time1, time2)
        table.add_row('Latency in us', latency)
        body = EventUtils.create_event_body(table)

        attach_ids = [MessageID(connection_id=ConnectionID(session_alias=proto_message['metadata']['session_alias']),
                                direction=proto_message['metadata']['direction'],
                                sequence=proto_message['metadata']['sequence'])]

        properties = ', '.join(proto_message['metadata']['properties'][key]
                               for key in self.included_properties
                               if key in proto_message['metadata']['properties'])

        return EventUtils.create_event(name=f'{self.latency_info} for message with {self.hash_field} = {hash_field} | '
                                            f'{properties}',
                                       status=EventStatus.SUCCESS,
                                       attached_message_ids=attach_ids,
                                       body=body)
