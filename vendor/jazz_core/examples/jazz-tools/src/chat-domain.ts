import {
  type AbiRowBatch,
  type DescriptorField,
  PostcardWriter,
  decodeRecordBytes,
  decodeRecordString,
  encodedCells,
  fieldIndex,
  writeValueType,
  utf8,
} from "./direct-codec.js";

export type RoomInput = {
  name: string;
};

export type RoomMemberInput = {
  room: Uint8Array;
  user: Uint8Array;
};

export type MessageInput = {
  room: Uint8Array;
  text: string;
  sender: Uint8Array;
};

export type MessageView = MessageInput & {
  rowId: Uint8Array;
};

export function chatSchema(): Uint8Array {
  const writer = new PostcardWriter();
  writer.vec((table, index) => {
    if (index === 0) writeTable(table, "rooms", roomDescriptor(), [], undefined);
    if (index === 1)
      writeTable(table, "room_members", roomMemberDescriptor(), [["room", "rooms"]], undefined);
    if (index === 2)
      writeTable(
        table,
        "messages",
        messageDescriptor(),
        [["room", "rooms"]],
        writeMessageReadPolicy,
      );
  }, 3);
  writer.none();
  writer.none();
  return writer.finish();
}

export function encodedRoomCells(room: RoomInput): Uint8Array {
  return encodedCells(roomDescriptor(), [utf8(room.name)]);
}

export function encodedRoomMemberCells(member: RoomMemberInput): Uint8Array {
  return encodedCells(roomMemberDescriptor(), [member.room, member.user]);
}

export function encodedMessageCells(message: MessageInput): Uint8Array {
  return encodedCells(messageDescriptor(), [message.room, utf8(message.text), message.sender]);
}

export function messageViews(batches: AbiRowBatch[]): MessageView[] {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => ({
      rowId: row.rowId,
      room: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "room")),
      text: decodeRecordString(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "text")),
      sender: decodeRecordBytes(batch.descriptor, row.raw, fieldIndex(batch.descriptor, "sender")),
    })),
  );
}

export function formatMessages(messages: MessageView[]): string {
  return messages.map((message) => message.text).join(", ") || "none";
}

function roomDescriptor(): DescriptorField[] {
  return [{ name: "name", valueType: { tag: 6 } }];
}

function roomMemberDescriptor(): DescriptorField[] {
  return [
    { name: "room", valueType: { tag: 8 } },
    { name: "user", valueType: { tag: 8 } },
  ];
}

function messageDescriptor(): DescriptorField[] {
  return [
    { name: "room", valueType: { tag: 8 } },
    { name: "text", valueType: { tag: 6 } },
    { name: "sender", valueType: { tag: 8 } },
  ];
}

function writeTable(
  writer: PostcardWriter,
  tableName: string,
  descriptor: DescriptorField[],
  references: [string, string][],
  writeReadPolicy: ((writer: PostcardWriter) => void) | undefined,
): void {
  writer.string(tableName);
  writer.vec((column, index) => {
    const columnSpec = descriptor[index];
    column.string(columnSpec.name ?? "");
    writeValueType(column, columnSpec.valueType);
    column.none();
  }, descriptor.length);
  writer.map(references.length);
  for (const [column, target] of references) {
    writer.string(column);
    writer.string(target);
  }
  if (writeReadPolicy) writer.some(writeReadPolicy);
  else writer.none();
  writer.none();
  writer.set(0);
  writer.map(0);
}

function writeMessageReadPolicy(writer: PostcardWriter): void {
  writer.string("messages");
  writer.vec(() => undefined, 0);
  writer.vec(writeRoomMembershipJoin, 1);
  writer.vec(() => undefined, 0);
  writer.vec(() => undefined, 0);
  writer.none();
  writer.vec(() => undefined, 0);
  writer.none();
  writer.none();
  writer.u64(0);
}

function writeRoomMembershipJoin(writer: PostcardWriter): void {
  writer.string("room_members");
  writer.string("room");
  writer.some((sourceColumn) => sourceColumn.string("room"));
  writer.vec(writeUserClaimFilter, 1);
}

function writeUserClaimFilter(writer: PostcardWriter): void {
  writer.enumUnit(3);
  writer.enumUnit(0);
  writer.string("user");
  writer.enumUnit(2);
  writer.string("sub");
}
