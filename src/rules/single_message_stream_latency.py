# Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
from typing import Dict, Any
import json

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


class HashInfo:

    def __init__(self, info: dict):
        self.hash_field = info.get('HashField', 'ClOrdID')
        self.is_property = info.get('IsProperty', False)
        self.is_multiple = info.get('IsMultiple', False)


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
        self.hash_info = HashInfo(configuration.get('HashInfo', {}))
        self.time1 = configuration.get('Time1', 'TransactTime')
        self.time2 = configuration.get('Time2', 'TransactTime')
        self.mode = LatencyCalculationMode(configuration.get('Mode', 'SendingTransact'))

        self.latency_info = configuration.get('LatencyInfo', 'Latency')
        self.latency_type = configuration.get('LatencyType', 'ResponseLatency')
        self.included_properties = configuration.get('Properties', [])
        self.publish_kafka_events = configuration.get('publish_kafka_events', False)
        self.kafka_key = configuration.get('kafka_key', 'default_key')
        self.kafka_topic = configuration.get('kafka_topic', 'default_topic')

    def kafka_client(self, kafka):
        self.kafka = kafka

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message['metadata']['message_type']
        session_alias: str = message.proto_message['metadata']['session_alias']

        if message_type in self.message_types and \
                (len(self.session_aliases) == 0 or any(fnmatch(session_alias, wildcard) for wildcard in self.session_aliases)):
            message.group_id = 'Message'

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        if self.hash_info.is_property:
            hash_field = message.proto_message['metadata']['properties'][self.hash_info.hash_field]
            message.hash_info[self.hash_info.hash_field] = hash_field
        elif self.hash_info.is_multiple:
            hash_field = ''
            for field in self.hash_info.hash_field:
                field_value = message.proto_message['fields'].get(field)
                if field_value is not None:
                    hash_field += field_value
                    message.hash_info[field] = field_value
        else:    
            hash_field = message.proto_message['fields'][self.hash_info.hash_field]
            message.hash_info[self.hash_info.hash_field] = hash_field

        message.hash = hash(hash_field)

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        message = messages[0]
        proto_message: Dict[str, Any] = message.proto_message
        message_type = proto_message['metadata']['message_type']
        if self.hash_info.is_property:
            hash_field = proto_message['metadata']['properties'][self.hash_info.hash_field]
        elif self.hash_info.is_multiple:
            hash_field = ', '.join(proto_message['fields'][field] 
                                   for field in self.hash_info.hash_field)
        else:    
            hash_field = proto_message['fields'][self.hash_info.hash_field]

        table = TableComponent(['Name', 'Value'])
        kafka_event = {}
        kafka_event["LatencyType", self.latency_type]
        kafka_event["Version", "1.0"]

        table.add_row('MessageType', message_type)

        kafka_event["RequestMessageType"] = message_type
        kafka_event["ResponseMessageType"] = None

        if self.hash_info.is_multiple:
            kafka_event["RequestMatchFields"] = self.hash_info.hash_field
            kafka_event["MatchValues"] = hash_field.split(", ")
        else:
            kafka_event["RequestMatchFields"] = [self.hash_info.hash_field]
            kafka_event["MatchValues"] = [hash_field]
        kafka_event["ResponseMatchFields"] = None

        kafka_event["ExecType"] = None
        kafka_event["OrdStatus"] = None


        table.add_row('Timestamp', str(proto_message['metadata']['timestamp']))
        table.add_row('Mode', str(self.mode).format(self.time2, self.time1))

        if self.mode == LatencyCalculationMode.CUSTOM:

            if self.time1 == 'SendingTime':
                time1 = proto_message['fields']['header']['SendingTime']
                kafka_event['LatencyStartTimeField'] = "SendingTime"
            else:
                time1 = proto_message['fields'][self.time1]
                kafka_event['LatencyStartTimeField'] = self.time1

            if self.time2 == 'SendingTime':
                time2 = proto_message['fields']['header']['SendingTime']
                kafka_event['LatencyEndTimeField'] = "SendingTime"
            else:
                time2 = proto_message['fields'][self.time2]
                kafka_event['LatencyEndTimeField'] = self.time2

        else:
            time1 = proto_message['fields']['TransactTime']
            kafka_event['LatencyStartTimeField'] = "TransactTime"
            time2 = proto_message['fields']['header']['SendingTime']
            kafka_event['LatencyEndTimeField'] = "SendingTime"

        kafka_event['LatencyStartTime'] = time1
        kafka_event['LatencyEndTime'] = time2
        table.add_row('Time 1', time1)
        table.add_row('Time 2', time2)

        latency = calculate_latency(time1, time2)
        table.add_row('Latency in us', str(latency))
        kafka_event['Latency'] = str(latency)
        body = EventUtils.create_event_body(table)

        attach_ids = [MessageID(connection_id=ConnectionID(session_alias=proto_message['metadata']['session_alias']),
                                direction=proto_message['metadata']['direction'],
                                sequence=proto_message['metadata']['sequence'])]

        properties = ', '.join(proto_message['metadata']['properties'][key]
                               for key in self.included_properties
                               if key in proto_message['metadata']['properties'])
        

        if self.kafka and self.publish_kafka_events:
            self.kafka.send_with_topic(self.kafka_topic, self.kafka_key, json.dumps(kafka_event))
            self.kafka.flush()

        return EventUtils.create_event(name=f'{self.latency_info} for message with {self.hash_info.hash_field} = {hash_field} | '
                                            f'{properties}',
                                       status=EventStatus.SUCCESS,
                                       attached_message_ids=attach_ids,
                                       body=body)
