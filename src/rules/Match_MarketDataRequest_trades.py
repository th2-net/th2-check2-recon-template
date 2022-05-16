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
from th2_check2_recon.common import EventUtils, TableComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType
from th2_grpc_common.common_pb2 import Direction, Event, EventStatus

logger = logging.getLogger(__name__)


def fill_field_value(conteiner: list, field: str, value1, value2):
    conteiner[0][field] = value1.simple_value
    conteiner[1][field] = value2.simple_value
    conteiner[2][field] = "PASSED" if value1.simple_value == value2.simple_value else "FAILED"


class Rule(rule.Rule):
    config = dict()

    def get_name(self) -> str:
        groups = set(self.config.values())
        name = "MarketDataRequest rule: "
        for group in groups:
            aliases = list()
            for alias in self.config:
                if self.config[alias] == group:
                    aliases.append(alias)
            name += str(aliases) + " vs "
        name = name[:-4]
        return name

    def get_description(self) -> str:
        return "ExecutionReport and MarketDataSnapshotFullRefresh have the same fields"

    def configure(self, configuration):
        self.config = configuration

    def get_attributes(self) -> [list]:
        return [
            ['parsed', 'subscribe']
        ]

    def description_of_groups(self) -> dict:
        return {'MDR-FIX': MessageGroupType.single,
                'ER-FIX': MessageGroupType.multi}

    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type
        session_alias = message.proto_message.metadata.id.connection_id.session_alias
        direction = message.proto_message.metadata.id.direction

        if session_alias not in self.config.keys() or message_type not in ['ExecutionReport',
                                                                           'MarketDataSnapshotFullRefresh']:
            return
        if message_type == 'ExecutionReport' and direction != Direction.FIRST:
            return
        if message_type == 'ExecutionReport' and (
                message.proto_message.fields["ClOrdID"].simple_value == '' or
                message.proto_message.fields["ExecType"].simple_value == '' or
                message.proto_message.fields["ExecType"].simple_value != 'F' or
                message.proto_message.fields["ExecID"].simple_value == ''):
            return
        if message_type == 'ExecutionReport':
            message.group_id = "ER-FIX"
        elif message_type == 'MarketDataSnapshotFullRefresh':
            message.group_id = "MDR-FIX"

    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        message_type: str = message.proto_message.metadata.message_type
        val = ''
        if message_type == 'ExecutionReport':
            security_id = message.proto_message.fields['SecurityID'].simple_value
            security_id_source = message.proto_message.fields['SecurityIDSource'].simple_value
            for field_name in ['SecurityID', 'SecurityIDSource']:
                val += message.proto_message.fields[field_name].simple_value
        if message_type == 'MarketDataSnapshotFullRefresh':
            security_id = message.proto_message.fields['Instrument'].message_value.fields['SecurityID'].simple_value
            security_id_source = message.proto_message.fields['Instrument'].message_value.fields[
                'SecurityIDSource'].simple_value
            for field_name in ['SecurityID', 'SecurityIDSource']:
                val += message.proto_message.fields['Instrument'].message_value.fields[field_name].simple_value
        message.hash = hash(val)
        message.hash_info['SecurityID'] = security_id
        message.hash_info['SecurityIDSource'] = security_id_source

    def check(self, messages: [ReconMessage], *args, **kwargs) -> Event:
        logger.info(f"RULE '{self.get_name()}': CHECK: input_messages: {messages}")

        info_for_name = dict()
        er = ReconMessage
        mdr = ReconMessage
        check = [{"SecurityID": None, "SecurityIDSource": None, "ID": None, "Price": None, "Qty": None,
                 "Buyer": None, "Seller": None, "Time": None},
                 {"SecurityID": None, "SecurityIDSource": None, "ID": None, "Price": None, "Qty": None,
                 "Buyer": None, "Seller": None, "Time": None},
                 {"SecurityID": None, "SecurityIDSource": None, "ID": None, "Price": None, "Qty": None,
                 "Buyer": None, "Seller": None, "Time": None}]

        msg: ReconMessage
        for msg in messages:
            info_for_name.update(msg.hash_info)
            message = msg.proto_message

            if message.metadata.message_type == "ExecutionReport":
                er = message

            elif message.metadata.message_type == "MarketDataSnapshotFullRefresh":
                mdr = message

        fill_field_value(check, "SecurityID", er.fields["SecurityID"],
                         mdr.fields["Instrument"].message_value.fields["SecurityID"])
        fill_field_value(check, "SecurityIDSource", er.fields["SecurityIDSource"],
                         mdr.fields["Instrument"].message_value.fields["SecurityIDSource"])

        value = mdr.fields["NoMDEntries"].list_value
        if er.fields["ClOrdID"].simple_value == \
                value.values[0].message_value.fields["MDEntryID"].simple_value:
            value = value.values[0]
        elif er.fields["ClOrdID"].simple_value == \
                value.values[1].message_value.fields["MDEntryID"].simple_value:
            value = value.values[1]

        fill_field_value(check, "ID", er.fields["ClOrdID"],
                         value.message_value.fields["MDEntryID"])
        fill_field_value(check, "Price", er.fields["Price"],
                         value.message_value.fields["MDEntryPx"])
        fill_field_value(check, "Qty", er.fields["OrderQty"],
                         value.message_value.fields["MDEntrySize"])

        party = er.fields["TradingParty"].message_value.fields["NoPartyIDs"].list_value
        for x in range(0, 4):
            if party.values[x].message_value.fields["PartyRole"].simple_value == '76':
                fill_field_value(check, "Buyer", party.values[x].message_value.fields["PartyID"],
                                 value.message_value.fields["MDEntryBuyer"])

            if party.values[x].message_value.fields["PartyRole"].simple_value == '17':
                fill_field_value(check, "Seller", party.values[x].message_value.fields["PartyID"],
                                 value.message_value.fields["MDEntrySeller"])

        fill_field_value(check, "Time", er.fields["TransactTime"],
                         value.message_value.fields["MDEntryTime"])

        time = er.fields["TransactTime"].simple_value
        if time[11::] == value.message_value.fields["MDEntryTime"].simple_value:
            check[2]["Time"] = "PASSED"

        table = TableComponent(['Name', 'ER Value', 'MDR Value', 'Status'])
        field = check[0].keys()
        for value in field:
            table.add_row(value, check[0][value], check[1][value], check[2][value])

        #event_message = f'mdr: {mdr.fields["MDReqID"].simple_value} vs er: {er.fields["ClOrdID"].simple_value}'
        body = EventUtils.create_event_body(table)
        attach_ids = [msg.proto_message.metadata.id for msg in messages]
        status = EventStatus.SUCCESS if ("FAILED" in check[2].values()) is False else EventStatus.FAILED
        return EventUtils.create_event(name=f"Match by '{ReconMessage.get_info(info_for_name)}'",
                                       status=status,
                                       attached_message_ids=attach_ids,
                                       body=body)
