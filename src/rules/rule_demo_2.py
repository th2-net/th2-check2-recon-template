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

from th2recon import rule
from th2recon.common import EventUtils
from th2recon.reconcommon import ReconMessage, MessageGroupType
from th2recon.th2 import infra_pb2, message_comparator_pb2
from th2recon.th2.infra_pb2 import Direction

logger = logging.getLogger()


class Rule(rule.Rule):

    def description_of_groups(self) -> dict:
        return {'ER_FIRST': MessageGroupType.multi,
                'NOS_SECOND': MessageGroupType.single}

    def group(self, message: ReconMessage):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        direction = message.proto_message.metadata.id.direction
        if session_alias not in ['arfq01fix01', 'arfq02fix01'] or \
                message_type not in ['ExecutionReport', 'NewOrderSingle']:
            return

        if (message_type == 'ExecutionReport' and direction != Direction.FIRST) or \
                (message_type == 'NewOrderSingle' and direction != Direction.SECOND):
            return

        message.group_id = message_type.translate({ord(c): '' for c in string.ascii_lowercase})
        message.group_id += '_' + Direction.Name(direction)

        message.group_info['session_alias'] = session_alias
        message.group_info['direction'] = direction

    def configure(self, configuration):
        pass

    def get_name(self) -> str:
        return "Rule_2"

    def get_description(self) -> str:
        return "Rule_2 is used for demo"

    def hash(self, message: ReconMessage):
        cl_ord_id = message.proto_message.fields['ClOrdID'].simple_value
        message.hash = hash(message.proto_message.fields['ClOrdID'].simple_value)
        message.hash_info['ClOrdID'] = cl_ord_id

    def check(self, messages: [ReconMessage]) -> infra_pb2.Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: ")
        settings = message_comparator_pb2.ComparisonSettings()
        try:
            messages = [msg.proto_message for msg in messages]
            comparison_result = self.message_comparator.compare(messages[0], messages[1], settings).result()
            hash_field_values = dict()
            for field_name in ['ClOrdID']:
                if not hash_field_values.__contains__(field_name):
                    hash_field_values[field_name] = []
                hash_field_values[field_name].append(messages[0].fields[field_name].simple_value)
                hash_field_values[field_name].append(messages[1].fields[field_name].simple_value)
            return EventUtils.create_verification_event(self.rule_event.id, comparison_result, hash_field_values)
        except Exception:
            logger.exception(
                f"Rule: {self.get_name()}. Error while send comparison request:\n"
                f"Expected:{messages[0]}.\n" +
                f"Actual:{messages[1]}. \n"
                f"Settings:{settings}")
