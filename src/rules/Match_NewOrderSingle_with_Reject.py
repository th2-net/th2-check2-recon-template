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

from th2_check2_recon import rule
from th2_check2_recon.common import EventUtils
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Direction, Event


logger = logging.getLogger(__name__)


class Rule(rule.Rule):
    config = dict()

    def get_name(self) -> str:
        groups = set(self.config.values())
        name = "Match trades rule: "
        for group in groups:
            aliases = list()
            for alias in self.config:
                if self.config[alias] == group:
                    aliases.append(alias)
            name += str(aliases) + " vs "
        name = name[:-4]
        return name

    def get_description(self) -> str:
        return "Trader1 and trader2 both receive ExecutionReport with the same TrdMatchID"

    def configure(self, configuration):
        self.config = configuration

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        desc = dict()
        groups = set(self.config.values())
        for group in groups:
            desc[group] = MessageGroupType.single
        return desc

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        direction = message.proto_message.metadata.id.direction
        if session_alias in self.config.keys() and message_type in ['Reject', 'ExecutionReport']:
            message.group_id = self.config[message_type]



    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        trd_match_id = message.proto_message.fields['TrdMatchID'].simple_value
        message.hash = hash(trd_match_id)
        message.hash_info['TrdMatchID'] = trd_match_id

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: input_messages: {messages}")

        ignore_fields = ['CheckSum', 'BodyLength', 'SendingTime', 'TargetCompID', 'PartyID', 'OrderID', 'CumQty',
                         'OrderQty', 'ExecID', 'LeavesQty', 'MsgSeqNum', 'Price', 'TimeInForce', 'Side', 'Text',
                         'OrdStatus', 'ClOrdID']
        verification_component = self.message_comparator.compare_messages(messages, ignore_fields)

        info_for_name = dict()
        for message in messages:
            info_for_name.update(message.hash_info)

        body = EventUtils.create_event_body(verification_component)
        attach_ids = [msg.proto_message.metadata.id for msg in messages]

        return EventUtils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}'",
                                       status=verification_component.status,
                                       attached_message_ids=attach_ids,
                                       body=body)
