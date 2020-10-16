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
from th2recon.common import TableComponent, EventUtils
from th2recon.reconcommon import MessageGroupType, ReconMessage
from th2recon.th2 import infra_pb2

logger = logging.getLogger()


class Rule(rule.Rule):

    def description_of_groups(self) -> dict:
        return {'NOS_arfq01fix01': MessageGroupType.single,
                'ER_arfq01fix01_0': MessageGroupType.single,
                'ER_arfq01fix01_F': MessageGroupType.single,
                'ER_arfq01dc01_0': MessageGroupType.single,
                'ER_arfq01dc01_F': MessageGroupType.single}

    def group(self, message: ReconMessage):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        if message_type not in ['ExecutionReport', 'NewOrderSingle'] or \
                session_alias not in ['arfq01fix01', 'arfq01dc01']:
            return

        message.group_id = message_type.translate({ord(c): '' for c in string.ascii_lowercase})
        message.group_id += '_' + session_alias
        message.group_info['session_alias'] = session_alias

        if message_type == 'ExecutionReport':
            exec_type = message.proto_message.fields['ExecType'].simple_value
            message.group_id += '_' + exec_type
            message.group_info['ExecType'] = exec_type

    def configure(self, configuration):
        pass

    def get_name(self) -> str:
        return "Rule_1"

    def get_description(self) -> str:
        return "Rule_1 is used for demo"

    def hash(self, message: ReconMessage):
        cl_ord_id = message.proto_message.fields['ClOrdID'].simple_value
        message.hash = hash(message.proto_message.fields['ClOrdID'].simple_value)
        message.hash_info['ClOrdID'] = cl_ord_id

    def check(self, messages: [ReconMessage]) -> infra_pb2.Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: ")

        table_component = TableComponent(['Session alias', 'MessageType', 'ExecType', 'ClOrdID', 'Group ID'])
        for msg in messages:
            msg_type = msg.proto_message.metadata.message_type
            exec_type = msg.proto_message.fields['ExecType'].simple_value
            cl_ord_id = msg.proto_message.fields['ClOrdID'].simple_value
            session_alias = msg.proto_message.metadata.id.connection_id.session_alias
            table_component.add_row(session_alias, msg_type, exec_type, cl_ord_id, msg.group_id)

        info_for_name = dict()
        for message in messages:
            info_for_name.update(message.hash_info)

        body = EventUtils.create_event_body(table_component)
        attach_ids = [msg.proto_message.metadata.id for msg in messages]
        return EventUtils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}' from 3 group",
                                       attached_message_ids=attach_ids,
                                       body=body)
