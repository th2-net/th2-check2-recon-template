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

import datetime
import logging

from th2recon import rule, store
from th2recon.th2 import infra_pb2

logger = logging.getLogger()


def hashed_fields() -> list:
    return ['ClOrdID']


class Rule(rule.Rule):

    def get_name(self) -> str:
        return "DVRule"

    def get_description(self) -> str:
        return "DVRule"

    def hash(self, message: infra_pb2.Message) -> str:
        if message.metadata.message_type == 'Heartbeat':
            return rule.IGNORED_HASH
        str_fields = ""
        for field_name in hashed_fields():
            if message.fields[field_name].simple_value == '':
                return rule.IGNORED_HASH
            str_fields += message.fields[field_name].simple_value
        return str(hash(message.metadata.message_type + str_fields))

    def check(self, messages_by_routing_key: dict) -> infra_pb2.Event:
        messages = []
        for key in self.routing_keys:
            messages.append(messages_by_routing_key[key])
        SendTime0 = messages[0].fields['header'].message_value.fields['SendingTime'].simple_value
        TranTime0 = messages[0].metadata.timestamp.seconds
        SendTime1 = messages[1].fields['header'].message_value.fields['SendingTime'].simple_value
        TranTime1 = messages[1].metadata.timestamp.seconds

        tran_time_0 = TranTime0 - int(datetime.datetime.strptime(SendTime0, '%Y-%m-%dT%H:%M:%S.%f').timestamp())
        tran_time_1 = TranTime1 - int(datetime.datetime.strptime(SendTime1, '%Y-%m-%dT%H:%M:%S.%f').timestamp())
        result = tran_time_0 == tran_time_1
        start_time = datetime.datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event_message = f'Check message. SendingTime0={SendTime0}\n' \
                        f'SendingTime1={SendTime1}\n' \
                        f'tran_time_0={tran_time_0}\n' \
                        f'tran_time_1={tran_time_1}\n' \
                        f'Event={result}'
        logger.debug(f'Eventmessage={event_message}')
        event = infra_pb2.Event(id=store.new_event_id(),
                                parent_id=self.rule_event_id,
                                name='Check',
                                start_timestamp=Timestamp(seconds=seconds, nanos=nanos),
                                status=infra_pb2.EventStatus.SUCCESS if result else infra_pb2.EventStatus.FAILED,
                                body=store.ComponentEncoder().encode(store.MessageComponent(event_message)).encode())
        event.attached_message_ids.append(messages[0].metadata.id)
        event.attached_message_ids.append(messages[1].metadata.id)
        return event
