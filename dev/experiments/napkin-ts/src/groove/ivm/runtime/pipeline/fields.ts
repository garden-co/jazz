// Process-local field routes. Total projections change this mapping, not row bytes.
import {
  BorrowedRecord,
  IvmRuntimeError,
  PreparedProjection,
  decodeSingleFieldValue,
  recordFieldLiteralOrdering,
  recordsError,
  resolveFieldName,
  type RecordDescriptor,
  type Value,
} from "../../../../_extern/groove-ivm-ops.js";

type Origin =
  | { kind: "Source"; path: [RecordDescriptor, number][] }
  | { kind: "Constant"; bytes: Uint8Array };

interface Field {
  origin: Origin;
  presentWrappers: number;
}

function fieldBytes(field: Field, raw: Uint8Array): Uint8Array {
  if (field.origin.kind === "Constant") return field.origin.bytes;
  let bytes = raw;
  for (const [descriptor, index] of field.origin.path) {
    const [start, end] = descriptor.fieldSpan(bytes, index);
    bytes = bytes.subarray(start, end);
  }
  return bytes;
}

const cloneField = (f: Field): Field => ({
  presentWrappers: f.presentWrappers,
  origin: f.origin.kind === "Source" ? { kind: "Source", path: f.origin.path.slice() } : f.origin,
});

export class FieldRoutes {
  constructor(
    private readonly source: RecordDescriptor,
    readonly descriptor: RecordDescriptor,
    private readonly fields: Field[],
    readonly reusesInput: boolean,
  ) {}

  static identity(descriptor: RecordDescriptor): FieldRoutes {
    const fields = descriptor.fields().map(
      (_: unknown, index: number): Field => ({
        origin: { kind: "Source", path: [[descriptor, index]] },
        presentWrappers: 0,
      }),
    );
    return new FieldRoutes(descriptor, descriptor, fields, true);
  }

  compose(descriptor: RecordDescriptor, projection: any): FieldRoutes | undefined {
    const fields: Field[] = [];
    for (const raw of projection.fields) {
      let field: Field;
      switch (raw.kind) {
        case "Copy": {
          const src = this.fields[raw.sourceIdx];
          if (!src) return undefined;
          field = cloneField(src);
          break;
        }
        case "WrapNullable": {
          const src = this.fields[raw.sourceIdx];
          if (!src) return undefined;
          field = cloneField(src);
          field.presentWrappers++;
          break;
        }
        case "Nested": {
          const [first, ...rest] = raw.path as [RecordDescriptor, number][];
          const src = first && this.fields[first[1]];
          // A nested record cannot be read through a nullable tag.
          if (!src || src.presentWrappers !== 0) return undefined;
          field = cloneField(src);
          if (field.origin.kind === "Source") field.origin.path.push(...rest);
          else {
            let value = field.origin.bytes;
            for (const [d, index] of rest) {
              const span = d.fieldSpan(value, index);
              if (!span) return undefined;
              value = value.subarray(span[0], span[1]);
            }
            field.origin = { kind: "Constant", bytes: value };
          }
          break;
        }
        case "Encoded":
          field = { origin: { kind: "Constant", bytes: raw.bytes }, presentWrappers: 0 };
          break;
        default:
          // Never elide an expression which can fail or omit a row.
          return undefined;
      }
      fields.push(field);
    }
    const copies = fields.every(
      (f) => f.origin.kind === "Source" && f.presentWrappers === 0 && f.origin.path.length === 1,
    )
      ? fields.map((f) => ({ kind: "Copy", sourceIdx: (f.origin as any).path[0][1] }))
      : undefined;
    const reusesInput =
      !!copies && new PreparedProjection(this.source, descriptor, copies).reusesInput;
    return new FieldRoutes(this.source, descriptor, fields, reusesInput);
  }

  append(raw: Uint8Array, output: number[]): [number, number] {
    return this.descriptor.writeProjectedFieldsInto(output, (index: number, out: number[]) => {
      const field = this.fields[index];
      for (let i = 0; i < field.presentWrappers; i++) out.push(1);
      out.push(...fieldBytes(field, raw));
    });
  }

  record(raw: Uint8Array): RoutedRecord {
    return new RoutedRecord(this, this.fields, raw);
  }
}

export class RoutedRecord {
  constructor(
    private readonly routes: FieldRoutes,
    private readonly fields: Field[],
    private readonly raw: Uint8Array,
  ) {}

  value(name: string): Value {
    const index = resolveFieldName(this.routes.descriptor, name);
    if (index === undefined) throw recordsError("FieldNotFound", name);
    const field = this.fields[index];
    let ty = this.routes.descriptor.fields()[index].valueType;
    for (let i = 0; i < field.presentWrappers; i++) {
      if (ty.kind !== "Nullable")
        throw new Error("internal error: entered unreachable code: validated field route");
      ty = ty.inner;
    }
    let value = decodeSingleFieldValue(fieldBytes(field, this.raw), ty);
    for (let i = 0; i < field.presentWrappers; i++) value = { kind: "Nullable", value };
    return value;
  }

  literalOrdering(name: string, literal: any): any {
    const index = resolveFieldName(this.routes.descriptor, name);
    if (index === undefined) throw new IvmRuntimeError("GraphFieldNotFound", name);
    const field = this.fields[index];
    if (field.presentWrappers === 0 && field.origin.kind === "Source" && field.origin.path.length) {
      const path = field.origin.path;
      let raw = this.raw;
      for (const [descriptor, i] of path.slice(0, -1)) {
        const [start, end] = descriptor.fieldSpan(raw, i);
        raw = raw.subarray(start, end);
      }
      const [descriptor, last] = path[path.length - 1];
      return recordFieldLiteralOrdering(new BorrowedRecord(raw, descriptor), last, literal);
    }
    return { kind: "Unsupported" };
  }
}
