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
        name = "NewOrderSingle FIX vs CSV: "
        for group in groups:
            aliases = list()
            for alias in self.config:
                if self.config[alias] == group:
                    aliases.append(alias)
            name += str(aliases) + " vs "
        name = name[:-4]
        return name

    def configure(self, configuration):
        self.config = configuration

    def get_description(self) -> str:
        return "NewOrderSingle messages written to the csv file and NewOrderSingle messages received by FIX protocol " \
               "are the same "

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def configure(self, configuration):
        self.config = configuration

    def description_of_groups(self) -> dict:
        desc = dict()
        groups = set(self.config.values())
        for group in groups:
            desc[group] = MessageGroupType.single
        return desc

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        if session_alias not in self.config.keys() or message_type not in ['NewOrderSingle', 'Csv_Message']:
            return

        if message_type in ['Csv_Message'] and message.proto_message.fields['CsvRecordType'].simple_value != 'D':
            return

        if message.proto_message.fields['ClOrdID'].simple_value == "":
            logger.info(f"RULE '{self.get_name()}'. Suitable message with empty ClOrdID: {message.proto_message}.")
            return
        logger.info(f"Message pass in recon: {message.proto_message}")
        message.group_id = self.config[session_alias]

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type
        if message_type == 'NewOrderSingle':
            cl_ord_id = message.proto_message.fields['ClOrdID'].simple_value
            val = message.proto_message.fields["ClOrdID"].simple_value
            message.hash = hash(val)
            message.hash_info['ClOrdID'] = cl_ord_id
        elif message_type == 'Csv_Message':
            cl_ord_id = message.proto_message.fields['ClOrdID'].simple_value
            val = message.proto_message.fields["ClOrdID"].simple_value
            message.hash = hash(val)
            message.hash_info['ClOrdID'] = cl_ord_id

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: input_messages: {messages}")

        ignore_fields = ['CheckSum', 'header', 'trailer', 'TradingParty', 'BodyLength', 'SendingTime', 'MsgSeqNum', 'CsvRecordType', 'ExecType','Text','OrdStatus','CumQty','LeavesQty','ExecID','OrderID']
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
