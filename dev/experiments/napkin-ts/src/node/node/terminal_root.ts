// Decoding contract for structured terminal roots.
import { fromUtf8 } from "../../support/bytes.js";
import {
  type CurrentRowPublicationField,
  type OutputOccurrenceId,
  type RecordDescriptor,
  ErrorCode,
  ObjectId,
  OutputOccurrenceId as Occ,
  nodeError,
  publicationPublicName,
} from "../../_extern/node-views.js";
import { assert } from "../../support/runtime.js";

export type TerminalRootCarrier = "CurrentRow" | "Logical";

export interface TerminalRootPublicField {
  publication: CurrentRowPublicationField;
  name: string;
  descriptorFieldName: string;
  slot: number;
  carrier: TerminalRootCarrier;
}

export interface TerminalRootLayout {
  id: string;
  rootDescriptor: RecordDescriptor;
  rootKeySlot: number;
  rootKeyFieldName: string;
  publicFields: TerminalRootPublicField[];
  carrier: TerminalRootCarrier;
  rootUnionArm: boolean;
}

export function terminalRootPublicationFields(
  layout: TerminalRootLayout,
): CurrentRowPublicationField[] {
  const fields: CurrentRowPublicationField[] = layout.rootDescriptor.fields().map((field: any) => ({
    kind: "ResultField",
    name: field.name,
    visibility: "HiddenMetadata",
  }));
  for (const field of layout.publicFields) {
    assert(
      publicationPublicName(field.publication) === field.name,
      "terminal publication name must match its public slot mapping",
    );
    fields[field.slot] = field.publication;
  }
  return fields;
}

const protocolError = (m: string) => nodeError(ErrorCode.Protocol, m);

export function terminalRootOccurrenceIdWithRootUnion(
  encoded: Uint8Array,
  rootUnionArm: boolean,
): OutputOccurrenceId {
  let cursor = 0;
  const uuidAt = () => {
    if (encoded[cursor] !== 10 || cursor + 17 > encoded.length) return undefined;
    const id = ObjectId.fromUuid(encoded.slice(cursor + 1, cursor + 17));
    cursor += 17;
    return id;
  };
  const orderedStringAt = (): string | undefined => {
    if (encoded[cursor] !== 6) return undefined;
    cursor++;
    const out: number[] = [];
    for (;;) {
      if (cursor >= encoded.length) return undefined;
      const byte = encoded[cursor++];
      if (byte !== 0) {
        out.push(byte);
        continue;
      }
      const next = encoded[cursor];
      if (next === 0) {
        cursor++;
        break;
      } else if (next === 0xff) {
        cursor++;
        out.push(0);
      } else return undefined;
    }
    try {
      return fromUtf8(new Uint8Array(out));
    } catch {
      return undefined;
    }
  };

  const root = uuidAt();
  if (root === undefined) throw protocolError("terminal root key must begin with a UUID");
  const joined: any[] = [];
  const unionArms: [number, string][] = [];
  if (rootUnionArm) {
    const label = orderedStringAt();
    if (label === undefined)
      throw protocolError("terminal root key contains an invalid root union discriminator");
    const actualRoot = uuidAt();
    if (actualRoot === undefined)
      throw protocolError("terminal root key contains no root union contributor");
    if (!ObjectId.equals(actualRoot, root))
      throw protocolError("terminal root key root union contributor disagrees with root UUID");
    unionArms.push([0, label]);
  }
  while (cursor < encoded.length) {
    let discriminator: string | undefined;
    if (encoded[cursor] === 6) {
      discriminator = orderedStringAt();
      if (discriminator === undefined)
        throw protocolError("terminal root key contains an invalid union discriminator");
    }
    const joinedId = uuidAt();
    if (joinedId === undefined)
      throw protocolError("terminal root key contains an unsupported component");
    if (discriminator !== undefined) unionArms.push([joined.length + 1, discriminator]);
    joined.push(joinedId);
  }
  if (unionArms.length === 0) return Occ.new_(root, joined);
  const id = Occ.withUnionArms(root, joined, unionArms);
  if (id === undefined)
    throw protocolError("terminal root key contains invalid union discriminators");
  return id;
}
