// Role-specific runtime result publication schemas. Execution identities remain local.
import { concatBytes, equalBytes } from "../../support/bytes.js";
import { Blake3Hasher } from "../../support/hash.js";
import {
  CurrentRow,
  DescriptorField,
  OwnedRecord,
  RecordDescriptor,
  RecordProjector,
  V,
  ValueType,
  decodePersistedRecordDescriptor,
  encodePersistedRecordDescriptor,
  nodeInvalidStoredValue,
  runtimeResultIdentityBytes,
  valueEquals,
  valueTypeEquals,
} from "../../_extern/node-policy-peer.js";

const RESULT_DESCRIPTOR_MAGIC = Uint8Array.of(0x4a, 0x52, 0x50, 0x44, 0x01); // b"JRPD\x01"

export enum ResultDescriptorRole {
  Current = 0,
  Aggregate = 1,
}

const err = (m: string) => nodeInvalidStoredValue(m);

export function encodeResultDescriptor(role: ResultDescriptorRole, descriptor: any): Uint8Array {
  validateRoleFields(role, descriptor);
  return concatBytes(
    RESULT_DESCRIPTOR_MAGIC,
    Uint8Array.of(role),
    encodePersistedRecordDescriptor(descriptor),
  );
}

function validateRoleFields(role: ResultDescriptorRole, descriptor: any): void {
  const invalid = () => err("result descriptor field roles are not canonical");
  let groupOrdinal = 0;
  let valueOrdinal = 0;
  let valuesStarted = false;
  let occurrenceOrdinal = 1;
  let lastUnionPosition: number | undefined;
  let fields: any[] = descriptor.fields();
  if (role === ResultDescriptorRole.Current) {
    const first = fields[0];
    if (
      !first ||
      first.name !== "metadata/row_uuid" ||
      !valueTypeEquals(first.valueType, ValueType.Uuid)
    )
      throw invalid();
    fields = fields.slice(1);
  }
  fields.forEach((field, currentOrdinal) => {
    const name: string | undefined = field.name;
    if (name === undefined) throw invalid();
    const first = name.indexOf("/");
    const second = first < 0 ? -1 : name.indexOf("/", first + 1);
    if (second < 0) throw invalid();
    const tag = name.slice(0, first);
    const ordinal = name.slice(first + 1, second);
    let expected: number;
    if (
      role === ResultDescriptorRole.Current &&
      ["source", "result", "provenance", "metadata"].includes(tag) &&
      !valuesStarted
    ) {
      expected = currentOrdinal;
    } else if (
      role === ResultDescriptorRole.Current &&
      tag === "occurrence" &&
      lastUnionPosition === undefined
    ) {
      if (!valueTypeEquals(field.valueType, ValueType.Uuid)) throw invalid();
      valuesStarted = true;
      expected = occurrenceOrdinal++;
    } else if (role === ResultDescriptorRole.Current && tag === "occurrence_union") {
      if (!/^\d+$/.test(ordinal)) throw invalid();
      const position = Number(ordinal);
      if (
        !valueTypeEquals(field.valueType, ValueType.String) ||
        position >= occurrenceOrdinal ||
        (lastUnionPosition !== undefined && position <= lastUnionPosition)
      )
        throw invalid();
      lastUnionPosition = position;
      expected = position;
    } else if (role === ResultDescriptorRole.Aggregate && tag === "group" && !valuesStarted) {
      expected = groupOrdinal++;
    } else if (role === ResultDescriptorRole.Aggregate && tag === "value") {
      valuesStarted = true;
      expected = valueOrdinal++;
    } else throw invalid();
    if (ordinal !== String(expected)) throw invalid();
  });
}

const canonicalize = (descriptor: any) =>
  decodePersistedRecordDescriptor(encodePersistedRecordDescriptor(descriptor));

function aggregateRoleSchema(schema: any): any {
  if (
    schema.groupNames.length !== schema.groupKeyFields.length ||
    schema.valueNames.length !== schema.valueFields.length
  )
    throw err("aggregate role schema has inconsistent identity counts");
  const fields: any[] = [];
  for (const [role, names, sourceFields] of [
    ["group", schema.groupNames, schema.groupKeyFields],
    ["value", schema.valueNames, schema.valueFields],
  ] as [string, string[], any[]][]) {
    names.forEach((name, ordinal) =>
      fields.push(
        DescriptorField.new(`${role}/${ordinal}/${name}`, sourceFields[ordinal].valueType),
      ),
    );
  }
  return canonicalize(RecordDescriptor.newWithFields(fields));
}

export type PreparedAggregatePayload = {
  descriptor: Uint8Array;
  record: Uint8Array;
  rowIdentity: Uint8Array;
  replacementIdentity: Uint8Array;
};

function aggregateRowIdentity(schema: any, canonical: any, values: any[]): Uint8Array {
  switch (schema.groupKeyFields.length) {
    case 0:
      return runtimeResultIdentityBytes(V.string("global"), ValueType.String);
    case 1:
      return runtimeResultIdentityBytes(values[0], canonical.fields()[0].valueType);
    default:
      throw err("aggregate payload has unsupported group identity");
  }
}

function u64le(n: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(n), true);
  return b;
}

function aggregateReplacementIdentity(
  schema: any,
  canonical: any,
  values: any[],
  descriptor: Uint8Array,
  record: Uint8Array,
): Uint8Array {
  switch (schema.valueFields.length) {
    case 0:
      return runtimeResultIdentityBytes(V.string("empty"), ValueType.String);
    case 1: {
      const index = schema.groupKeyFields.length;
      return runtimeResultIdentityBytes(values[index], canonical.fields()[index].valueType);
    }
    default: {
      const h = new Blake3Hasher()
        .update(new TextEncoder().encode("jazz aggregate replacement v1"))
        .update(u64le(descriptor.length))
        .update(descriptor)
        .update(u64le(record.length))
        .update(record);
      return runtimeResultIdentityBytes(V.bytes(h.finalize()), ValueType.Bytes);
    }
  }
}

export function encodeAggregatePayloadRecord(record: any, schema: any): PreparedAggregatePayload {
  const runtimeFields = [...schema.groupKeyFields, ...schema.valueFields];
  const values = runtimeFields.map((field) => {
    if (field.identity === undefined) throw err("aggregate role field has no execution binding");
    const index = record.descriptor().fieldIndexByIdentity(field.identity);
    if (index === undefined) throw err("aggregate record is missing its compiled role field");
    if (!valueTypeEquals(record.descriptor().fields()[index].valueType, field.valueType))
      throw err("aggregate role field type differs from compiler schema");
    return record.getIdx(index);
  });
  const raw: Uint8Array = RecordDescriptor.newWithFields(runtimeFields).create(values);
  const canonical = aggregateRoleSchema(schema);
  const canonicalValues = canonical.bind(raw).toValues();
  if (!equalBytes(canonical.create(canonicalValues), raw))
    throw err("aggregate payload row is not canonical");
  const descriptor = encodeResultDescriptor(ResultDescriptorRole.Aggregate, canonical);
  return {
    descriptor,
    record: raw,
    rowIdentity: aggregateRowIdentity(schema, canonical, canonicalValues),
    replacementIdentity: aggregateReplacementIdentity(
      schema,
      canonical,
      canonicalValues,
      descriptor,
      raw,
    ),
  };
}

export function decodeAggregatePayloadRecord(payload: any, schema: any): any {
  const canonical = aggregateRoleSchema(schema);
  if (
    !equalBytes(
      encodeResultDescriptor(ResultDescriptorRole.Aggregate, canonical),
      payload.descriptor,
    )
  )
    throw err("aggregate payload role schema differs from compiled output");
  const values = canonical.bind(payload.record).toValues();
  if (!equalBytes(canonical.create(values), payload.record))
    throw err("aggregate payload row is not canonical");
  const member = payload.member;
  if (member.kind !== "Synthetic") throw err("aggregate payload has no synthetic member identity");
  if (
    !equalBytes(aggregateRowIdentity(schema, canonical, values), member.row) ||
    !equalBytes(
      aggregateReplacementIdentity(schema, canonical, values, payload.descriptor, payload.record),
      member.replacement.encodedRecord(),
    )
  )
    throw err("aggregate payload differs from its member identity");
  const runtime = RecordDescriptor.newWithFields([...schema.groupKeyFields, ...schema.valueFields]);
  return OwnedRecord.new(payload.record.slice(), runtime);
}

function currentRoleSchema(schema: any): any {
  const occ: string[] = schema.occurrenceIdFields;
  if (
    occ[0] !== schema.rowField ||
    new Set(occ).size !== occ.length ||
    [...schema.occurrenceUnionArmFields.keys()].some((p: number) => p >= occ.length)
  )
    throw err("result occurrence source declarations are invalid");
  const fields = [DescriptorField.new("metadata/row_uuid", ValueType.Uuid)];
  schema.payloadFields
    .filter((f: any) => f.name !== schema.rowField)
    .forEach((field: any, ordinal: number) => {
      const binding = schema.payloadPublicationFields.get(field.name);
      if (!binding) throw err("result payload field has no publication identity");
      let role: string;
      let name: string;
      if (binding.kind === "StoredColumn" || binding.kind === "UnresolvedSourceCell") {
        role = "source";
        name = binding.outputName;
      } else {
        name = binding.name;
        role =
          binding.visibility === "ApplicationCell"
            ? "result"
            : binding.visibility === "PublicProvenance"
              ? "provenance"
              : "metadata";
      }
      fields.push(DescriptorField.new(`${role}/${ordinal}/${name}`, field.ty));
    });
  occ.forEach((name, ordinal) => {
    if (ordinal > 0)
      fields.push(DescriptorField.new(`occurrence/${ordinal}/${name}`, ValueType.Uuid));
  });
  for (const [position, name] of schema.occurrenceUnionArmFields)
    fields.push(DescriptorField.new(`occurrence_union/${position}/${name}`, ValueType.String));
  return canonicalize(RecordDescriptor.newWithFields(fields));
}

/** One compiled field mapping per source descriptor and publication schema. */
export class CurrentPayloadEncodePlan {
  private constructor(
    private readonly projector: any,
    private readonly descriptor: Uint8Array,
  ) {}

  static new(descriptor: any, schema: any): CurrentPayloadEncodePlan {
    const selected: string[] = [
      schema.rowField,
      ...schema.payloadFields
        .filter((f: any) => f.name !== schema.rowField)
        .map((f: any) => f.name),
      ...schema.occurrenceIdFields.slice(1),
      ...schema.occurrenceUnionArmFields.values(),
    ];
    const mapping: [number, number][] = [];
    const runtimeFields: any[] = [];
    const sourceFields: any[] = descriptor.fields();
    for (const name of selected) {
      const index = sourceFields.findIndex((f) => f.name === name);
      if (index < 0) throw err("result payload is missing its declared carrier");
      if (sourceFields.slice(index + 1).some((f) => f.name === name))
        throw err("result payload carrier binding is ambiguous");
      mapping.push([index, mapping.length]);
      runtimeFields.push(sourceFields[index]);
    }
    const canonical = currentRoleSchema(schema);
    const canonicalFields: any[] = canonical.fields();
    runtimeFields.forEach((source, i) => {
      const target = canonicalFields[i];
      if (!target) return;
      const s = encodePersistedRecordDescriptor(
        RecordDescriptor.new([["value", source.valueType]]),
      );
      const t = encodePersistedRecordDescriptor(
        RecordDescriptor.new([["value", target.valueType]]),
      );
      if (!equalBytes(s, t)) throw err("result payload type differs from its publication schema");
    });
    return new CurrentPayloadEncodePlan(
      RecordProjector.newRegistryRebound(descriptor, canonical, mapping),
      encodeResultDescriptor(ResultDescriptorRole.Current, canonical),
    );
  }

  encode(record: any): [Uint8Array, Uint8Array] {
    return [this.descriptor, this.projector.project(record).intoRaw()];
  }
}

export function decodeCurrentPayloadRecord(table: string, payload: any, schema: any): any {
  const canonical = currentRoleSchema(schema);
  if (
    !equalBytes(encodeResultDescriptor(ResultDescriptorRole.Current, canonical), payload.descriptor)
  )
    throw err("result payload role schema differs from compiled output");
  const values: any[] = canonical.bind(payload.record).toValues();
  if (!equalBytes(canonical.create(values), payload.record))
    throw err("result payload row is not canonical");
  const asRow = payload.member.asRow();
  if (!asRow || !valueEquals(values[0], V.uuid(asRow[1])))
    throw err("result payload differs from its member row identity");
  const hidden = (name: string) => ({ kind: "ResultField", name, visibility: "HiddenMetadata" });
  const fields = [DescriptorField.new("row_uuid", ValueType.Uuid)];
  const publications: any[] = [hidden("row_uuid")];
  for (const field of schema.payloadFields.filter((f: any) => f.name !== schema.rowField)) {
    fields.push(DescriptorField.new(field.name, field.ty));
    const publication = schema.payloadPublicationFields.get(field.name);
    if (!publication) throw err("result payload field has no publication identity");
    publications.push(publication);
  }
  const occ: string[] = schema.occurrenceIdFields;
  const memberOccurrence = payload.member.outputOccurrenceId();
  if (
    (memberOccurrence ? memberOccurrence.joinedSources().length : 0) + 1 !== occ.length ||
    (memberOccurrence &&
      (!valueEquals(values[0], V.uuid(memberOccurrence.rootSource().uuid())) ||
        memberOccurrence.unionArms().length !== schema.occurrenceUnionArmFields.size))
  )
    throw err("result occurrence differs from compiled sources");
  if (occ.length > 1) {
    const occurrence = memberOccurrence;
    if (!occurrence) throw err("joined result payload has no occurrence identity");
    const joined: any[] = occurrence.joinedSources();
    const arms: [number, string][] = occurrence.unionArms();
    if (joined.length + 1 !== occ.length || arms.length !== schema.occurrenceUnionArmFields.size)
      throw err("result occurrence differs from compiled sources");
    occ.slice(1).forEach((name, i) => {
      if (!valueEquals(values[fields.length], V.uuid(joined[i].uuid())))
        throw err("result payload differs from its joined source identity");
      fields.push(DescriptorField.new(name, ValueType.Uuid));
      publications.push(hidden(name));
    });
    [...schema.occurrenceUnionArmFields].forEach(
      ([position, name]: [number, string], i: number) => {
        const [memberPosition, arm] = arms[i];
        if (position !== memberPosition || !valueEquals(values[fields.length], V.string(arm)))
          throw err("result payload differs from its union discriminator");
        fields.push(DescriptorField.new(name, ValueType.String));
        publications.push(hidden(name));
      },
    );
  }
  const descriptor = RecordDescriptor.newWithFields(fields);
  return CurrentRow.newWithPublicationFields(
    table,
    OwnedRecord.new(descriptor.create(values), descriptor),
    publications,
  );
}
