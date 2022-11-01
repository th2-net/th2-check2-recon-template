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
import string
from typing import List

from th2_common_utils import event_utils

from th2_check2_recon import rule
from th2_check2_recon.common import TableComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupDescription, ReconMessageUtils
from th2_grpc_common.common_pb2 import Event

logger = logging.getLogger(__name__)


class Rule(rule.Rule):

    def get_name(self) -> str:
        return 'Rule_1'

    def get_description(self) -> str:
        return 'Example rule'

    def get_attributes(self) -> List[str]:
        return ['parsed', 'subscribe']

    def description_of_groups(self) -> dict:
        return {
            'NOS_arfq01fix04': MessageGroupDescription(single=True),
            'ER_arfq01fix04_0': MessageGroupDescription(single=True),
            'ER_arfq01fix04_F': MessageGroupDescription(single=True),
            'ER_arfq01dc04_0': MessageGroupDescription(single=True),
            'ER_arfq01dc04_F': MessageGroupDescription(single=True)
        }

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = ReconMessageUtils.get_message_type(message)
        session_alias = ReconMessageUtils.get_session_alias(message)
        if message_type not in ['ExecutionReport', 'NewOrderSingle'] or \
                session_alias not in ['arfq01fix04', 'arfq01dc04']:
            return

        message.group_name = message_type.translate({ord(c): '' for c in string.ascii_lowercase})
        message.group_name += '_' + session_alias
        message.group_info['session_alias'] = session_alias

        if message_type == 'ExecutionReport':
            exec_type = ReconMessageUtils.get_value(message, 'ExecType')
            message.group_name += '_' + exec_type
            message.group_info['ExecType'] = exec_type

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        cl_ord_id = ReconMessageUtils.get_value(message, 'ClOrdID')
        message.hash = hash(cl_ord_id)
        message.hash_info['ClOrdID'] = cl_ord_id

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: ")

        table_component = TableComponent(['Session alias', 'MessageType', 'ExecType', 'ClOrdID', 'Group ID'])
        for msg in messages:
            msg_type = msg.proto_message.metadata.message_type
            exec_type = ReconMessageUtils.get_value(msg, 'ExecType')
            cl_ord_id = ReconMessageUtils.get_value(msg, 'ClOrdID')
            session_alias = ReconMessageUtils.get_session_alias(msg)
            table_component.add_row(session_alias, msg_type, exec_type, cl_ord_id, msg.group_name)

        info_for_name = dict()
        for message in messages:
            info_for_name.update(message.hash_info)

        body = event_utils.create_event_body(table_component)
        attach_ids = [ReconMessageUtils.get_message_id(msg) for msg in messages]
        return event_utils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}' from 3 group",
                                        attached_message_ids=attach_ids,
                                        body=body)
