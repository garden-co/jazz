import type { AccountCommand, Principal } from "../account_registry.js";
import { authorIdentityDescriptor as principalDescriptor } from "../ids.js";
import { principalIsNonempty } from "../identity.js";
import { equalBytes } from "../../support/bytes.js";
import {
  EnumCase,
  EnumSchema,
  EnumValue,
  OwnedRecord,
  RecordDescriptor,
  Value,
  ValueType,
} from "../../_extern/types-protocol.js";

const MAX_COMPONENT = 16 * 1024;

function schema(): any {
  const principal = () => ValueType.Record(principalDescriptor());
  return EnumSchema.new("jazz.account-command.v1", [
    EnumCase.new(
      "Register",
      new RecordDescriptor([
        ["principal", principal()],
        ["account", ValueType.Uuid],
      ]),
    ),
    EnumCase.new(
      "RequestLink",
      new RecordDescriptor([
        ["approver", principal()],
        ["candidate", principal()],
        ["nonce", ValueType.Uuid],
        ["now", ValueType.U64],
        ["expires_at", ValueType.U64],
      ]),
    ),
    EnumCase.new(
      "AcceptLink",
      new RecordDescriptor([
        ["candidate", principal()],
        ["nonce", ValueType.Uuid],
        ["now", ValueType.U64],
      ]),
    ),
    EnumCase.new(
      "Revoke",
      new RecordDescriptor([
        ["approver", principal()],
        ["target", principal()],
      ]),
    ),
    EnumCase.new(
      "FoundLocalFirst",
      new RecordDescriptor([
        ["principal", principal()],
        ["app", ValueType.Uuid],
      ]),
    ),
  ]).withRegistryId(1);
}

export function descriptor(): any {
  return new RecordDescriptor([["command", ValueType.Enum(schema())]]);
}

function principalValue(p: Principal): any {
  for (const c of [p.issuer, p.subject])
    if (c.length > MAX_COMPONENT || !principalIsNonempty(c))
      throw new Error("invalid account principal component");
  const d = principalDescriptor();
  return Value.Record(
    new OwnedRecord(d.create([Value.String(p.issuer), Value.String(p.subject)]), d),
  );
}

export function encode(command: AccountCommand): Uint8Array {
  let tag: number;
  let values: any[];
  switch (command.kind) {
    case "Register":
      [tag, values] = [0, [principalValue(command.principal), Value.Uuid(command.account)]];
      break;
    case "RequestLink":
      [tag, values] = [
        1,
        [
          principalValue(command.approver),
          principalValue(command.candidate),
          Value.Uuid(command.nonce),
          Value.U64(command.now),
          Value.U64(command.expiresAt),
        ],
      ];
      break;
    case "AcceptLink":
      [tag, values] = [
        2,
        [principalValue(command.candidate), Value.Uuid(command.nonce), Value.U64(command.now)],
      ];
      break;
    case "Revoke":
      [tag, values] = [3, [principalValue(command.approver), principalValue(command.target)]];
      break;
    case "FoundLocalFirst":
      [tag, values] = [4, [principalValue(command.principal), Value.Uuid(command.app)]];
      break;
  }
  const payload = schema().cases[tag].payload;
  return descriptor().create([Value.Enum(EnumValue.create(tag, payload, values))]);
}

function principal(value: any): Principal {
  if (value?.kind !== "Record") throw new Error("invalid account principal record");
  const [issuer, subject] = value.record.toValues();
  if (issuer?.kind !== "String" || subject?.kind !== "String")
    throw new Error("invalid account principal fields");
  return { issuer: issuer.value, subject: subject.value };
}

export function decode(bytes: Uint8Array): AccountCommand {
  const v = descriptor().getIdx(bytes, 0);
  if (v?.kind !== "Enum") throw new Error("invalid account command record");
  const vals: any[] = v.value.record().toValues();
  const is = (i: number, k: string) => vals[i]?.kind === k;
  let command: AccountCommand;
  const tag = v.value.tag();
  if (tag === 0 && vals.length === 2 && is(1, "Uuid"))
    command = { kind: "Register", principal: principal(vals[0]), account: vals[1].value };
  else if (tag === 1 && vals.length === 5 && is(2, "Uuid") && is(3, "U64") && is(4, "U64"))
    command = {
      kind: "RequestLink",
      approver: principal(vals[0]),
      candidate: principal(vals[1]),
      nonce: vals[2].value,
      now: vals[3].value,
      expiresAt: vals[4].value,
    };
  else if (tag === 2 && vals.length === 3 && is(1, "Uuid") && is(2, "U64"))
    command = {
      kind: "AcceptLink",
      candidate: principal(vals[0]),
      nonce: vals[1].value,
      now: vals[2].value,
    };
  else if (tag === 3 && vals.length === 2)
    command = { kind: "Revoke", approver: principal(vals[0]), target: principal(vals[1]) };
  else if (tag === 4 && vals.length === 2 && is(1, "Uuid"))
    command = { kind: "FoundLocalFirst", principal: principal(vals[0]), app: vals[1].value };
  else throw new Error("unknown account command");
  if (!equalBytes(encode(command), bytes)) throw new Error("noncanonical account command");
  return command;
}
