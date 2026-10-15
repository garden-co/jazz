import { Reader, Writer } from "../../support/postcard.js";
import type { RecordDescriptor, ValueType } from "../../_extern/db-core.js";
import {
  EnumCase,
  EnumSchema,
  RecordDescriptorT,
  decodeScalarValueType,
  encodeScalarValueType,
} from "../../_extern/db-core.js";

export function writeNativeValueType(w: Writer, value: ValueType): void {
  switch (value.kind) {
    case "Tuple":
      w.variant(13).seq(value.members, writeNativeValueType);
      return;
    case "Array":
      w.variant(14);
      writeNativeValueType(w, value.inner);
      return;
    case "Nullable":
      w.variant(15);
      writeNativeValueType(w, value.inner);
      return;
    case "Record":
      w.variant(16);
      writeNativeDescriptor(w, value.descriptor);
      return;
    case "Enum": {
      const schema = value.schema;
      w.variant(17).varint(schema.registryId).str(schema.name);
      w.seq(schema.cases, (w, c: any) => {
        w.str(c.name);
        writeNativeDescriptor(w, c.payload);
      });
      return;
    }
    default:
      encodeScalarValueType(w, value);
  }
}

export function writeNativeDescriptor(w: Writer, descriptor: RecordDescriptor): void {
  w.seq(descriptor.fields(), (w, field: any) => {
    w.option(field.name, (w, n: string) => w.str(n));
    writeNativeValueType(w, field.valueType);
  });
}

class Budget {
  constructor(public left: number) {}
}

export function readDescriptor(input: Reader): RecordDescriptor {
  return readFields(input, new Budget(1024), 0);
}

function count(input: Reader, budget: number): number {
  const n = input.varint();
  if (n > budget || n > input.remaining) throw new Error("binding descriptor node limit exceeded");
  return n;
}

function readFields(input: Reader, budget: Budget, depth: number): RecordDescriptor {
  const n = count(input, budget.left);
  const fields: any[] = [];
  for (let i = 0; i < n; i++) {
    const name = input.option((r) => r.str());
    const valueType = readType(input, budget, depth);
    fields.push({
      identity: name === undefined ? undefined : { kind: "Name", name },
      name,
      valueType,
    });
  }
  return RecordDescriptorT.newWithFields(fields);
}

function readType(input: Reader, budget: Budget, depth: number): ValueType {
  if (budget.left === 0 || depth >= 128)
    throw new Error("binding descriptor node or depth limit exceeded");
  budget.left--;
  const tag = input.varint();
  if (tag <= 12) return decodeScalarValueType(input, tag);
  switch (tag) {
    case 13: {
      const n = count(input, budget.left);
      const members: ValueType[] = [];
      for (let i = 0; i < n; i++) members.push(readType(input, budget, depth + 1));
      return { kind: "Tuple", members };
    }
    case 14:
      return { kind: "Array", inner: readType(input, budget, depth + 1) };
    case 15:
      return { kind: "Nullable", inner: readType(input, budget, depth + 1) };
    case 16:
      return { kind: "Record", descriptor: readFields(input, budget, depth + 1) };
    case 17: {
      const registryId = input.varint();
      const name = input.str();
      const n = count(input, budget.left);
      const cases: any[] = [];
      for (let i = 0; i < n; i++) {
        if (budget.left === 0) throw new Error("binding descriptor node limit exceeded");
        budget.left--;
        const caseName = input.str();
        cases.push(EnumCase.new(caseName, readFields(input, budget, depth + 1)));
      }
      return { kind: "Enum", schema: EnumSchema.new(name, cases).withRegistryId(registryId) };
    }
    default:
      throw new Error("unknown binding value type");
  }
}
