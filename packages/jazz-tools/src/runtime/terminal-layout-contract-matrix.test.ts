/**
 * Fast contract matrix for Rust's TerminalRootLayout -> TypeScript decoder
 * boundary.  It deliberately uses the postcard descriptor and packed-record
 * codec directly: no NAPI, WASM, browser, or generated artifact is involved.
 *
 * Rust's corresponding layout construction lives in
 * `maintained_subscription_view::terminal_root_layout`.  The dimensions below
 * mirror the producer choices it makes. This is a bounded producer-shaped
 * matrix, not an exhaustive cross-product: every projection/include changes
 * an actual descriptor, physical mapping, and packed operation record.
 *
 * TS-only boundary: Rust must eventually generate golden layouts/records for
 * these cases, then this test must consume them. Until then this protects the
 * TypeScript registration and packed-record contract only.
 */
import { describe, expect, it } from "vitest";
import type {
  ColumnDescriptor,
  NativeRowDelta,
  NativeTerminalRootLayout,
  Value,
  WasmRow,
} from "../drivers/types.js";
import { SubscriptionManager } from "./subscription-manager.js";
import {
  compileNativeTerminalRootDecoder,
  createRecord,
  encodeNativeRowValues,
  logicalStorageColumns,
  readDescriptor,
  storageColumnValueType,
  writeDescriptor,
  type DescriptorField,
} from "./native-runtime/native-row-codec.js";
import { PostcardReader, PostcardWriter } from "./native-runtime/native-codec.js";

const ROOT_ID = "00000000-0000-4000-8000-000000000001";

type Carrier = "Logical" | "CurrentRow";
type Shape = "scalar" | "array" | "row" | "nested-nullable";
type Include =
  | "plain"
  | "forward"
  | "reverse"
  | "multi-hop"
  | "nested-collector"
  | "sibling-collectors";

interface MatrixCase {
  carrier: Carrier;
  nullable: boolean;
  sparse: boolean;
  projection: "collect-all" | "explicit";
  include: Include;
}

const shapes: Record<Shape, ColumnDescriptor> = {
  scalar: { name: "title", column_type: { type: "Text" }, nullable: false },
  array: {
    name: "tags",
    column_type: { type: "Array", element: { type: "Text" } },
    nullable: false,
  },
  row: {
    name: "author",
    column_type: {
      type: "Row",
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    nullable: false,
  },
  "nested-nullable": {
    name: "profile",
    column_type: {
      type: "Row",
      columns: [{ name: "nickname", column_type: { type: "Text" }, nullable: true, sparse: true }],
    },
    nullable: true,
    sparse: true,
  },
};

function uuidBytes(id: string): Uint8Array {
  return Uint8Array.from(
    id
      .replaceAll("-", "")
      .match(/../g)!
      .map((part) => Number.parseInt(part, 16)),
  );
}

function encodedDescriptor(fields: DescriptorField[]): number[] {
  const writer = new PostcardWriter();
  writeDescriptor(writer, fields);
  return [...writer.finish()];
}

function descriptorFor(
  columns: readonly ColumnDescriptor[],
  physicalNames: readonly string[],
  carriers: readonly Carrier[],
): DescriptorField[] {
  return [
    { name: "row_uuid", valueType: { tag: 11 } },
    ...logicalStorageColumns(columns).map((column, index) => ({
      name: physicalNames[index]!,
      // CurrentRow adds a carrier around the already-declared nullable type;
      // nullable public cells therefore intentionally have two wrappers.
      valueType:
        carriers[index] === "CurrentRow"
          ? { tag: 15, inner: storageColumnValueType(column) }
          : storageColumnValueType(column),
    })),
    { name: "__route_provenance", valueType: { tag: 11 } },
  ];
}

function layoutFor(
  columns: readonly ColumnDescriptor[],
  physicalNames: readonly string[],
  carriers: readonly Carrier[],
  carrier: Carrier,
  id: string,
): NativeTerminalRootLayout {
  return {
    id,
    rootDescriptor: encodedDescriptor(descriptorFor(columns, physicalNames, carriers)),
    rootKeySlot: 0,
    rootKeyFieldName: "row_uuid",
    publicFields: columns.map((column, index) => ({
      name: column.name,
      descriptorFieldName: physicalNames[index]!,
      slot: index + 1,
      carrier: carriers[index]!,
    })),
    carrier,
  };
}

function emptyDelta(layouts: NativeTerminalRootLayout[]): NativeRowDelta {
  return {
    __jazzNativeRowDelta: true,
    added: new Uint8Array(),
    removed: new Uint8Array(),
    updated: new Uint8Array(),
    addedCount: 0,
    removedCount: 0,
    updatedCount: 0,
    terminalLayouts: layouts,
  };
}

// 2 carriers * nullable * sparse * projection * include family = 96 real
// packed root records. Include families add logical slots; explicit projection
// aliases the scalar physical slot rather than only changing an ID.
const cases: MatrixCase[] = (["Logical", "CurrentRow"] as const).flatMap((carrier) =>
  ([false, true] as const).flatMap((nullable) =>
    ([false, true] as const).flatMap((sparse) =>
      (["collect-all", "explicit"] as const).flatMap((projection) =>
        (
          [
            "plain",
            "forward",
            "reverse",
            "multi-hop",
            "nested-collector",
            "sibling-collectors",
          ] as const
        ).map((include): MatrixCase => ({ carrier, nullable, sparse, projection, include })),
      ),
    ),
  ),
);

describe("TerminalRootLayout encoding contract matrix", () => {
  it("covers the declared producer space with a non-empty exact matrix", () => {
    expect(cases).toHaveLength(96);
    expect(new Set(cases.map((entry) => entry.carrier))).toEqual(
      new Set(["Logical", "CurrentRow"]),
    );
    const contracts = new Set(
      cases.map((entry) => {
        const includeCount =
          entry.include === "plain" ? 0 : entry.include === "sibling-collectors" ? 2 : 1;
        const columns: ColumnDescriptor[] = [
          {
            name: "title",
            column_type: { type: "Text" },
            nullable: entry.nullable,
            sparse: entry.sparse || undefined,
          },
          ...Array.from({ length: includeCount }, (_, child) => ({
            name: `${entry.include}_${child}`,
            column_type: { type: "Text" as const },
            nullable: false,
          })),
        ];
        const physicalNames = [
          entry.projection === "explicit" ? "projected_title" : "user_title",
          ...Array.from({ length: includeCount }, (_, child) => `${entry.include}_${child}`),
        ];
        const carriers: Carrier[] = [
          entry.carrier,
          ...Array.from({ length: includeCount }, (): Carrier => "Logical"),
        ];
        const layout = layoutFor(
          columns,
          physicalNames,
          carriers,
          entry.carrier,
          "identity-is-not-contract",
        );
        return JSON.stringify({ ...layout, id: undefined });
      }),
    );
    // sparse is normalized out of terminal layouts by design, so it changes
    // input metadata but not this serialized root contract.
    expect(contracts).toHaveLength(48);
  });

  it("validates scalar, array, row, and nested-nullable descriptor types", () => {
    for (const carrier of ["Logical", "CurrentRow"] as const) {
      for (const column of Object.values(shapes)) {
        const layout = layoutFor(
          [column],
          [`user_${column.name}`],
          [carrier],
          carrier,
          `type-${carrier}-${column.name}`,
        );
        expect(() =>
          compileNativeTerminalRootDecoder(
            layout,
            readDescriptor(new PostcardReader(Uint8Array.from(layout.rootDescriptor))),
            [column],
          ),
        ).not.toThrow();
      }
    }
  });

  it("decodes every producer-shaped layout through the manager and excludes hidden route/provenance slots", () => {
    for (const [index, entry] of cases.entries()) {
      const column: ColumnDescriptor = {
        name: "title",
        column_type: { type: "Text" },
        nullable: entry.nullable,
        sparse: entry.sparse || undefined,
      };
      const includeCount =
        entry.include === "plain" ? 0 : entry.include === "sibling-collectors" ? 2 : 1;
      const includes: ColumnDescriptor[] = Array.from({ length: includeCount }, (_, child) => ({
        name: `${entry.include}_${child}`,
        column_type: { type: "Text" },
        nullable: false,
      }));
      const columns = [column, ...includes];
      const physicalNames = [
        entry.projection === "explicit" ? "projected_title" : "user_title",
        ...includes.map((include) => include.name),
      ];
      const carriers: Carrier[] = [entry.carrier, ...includes.map((): Carrier => "Logical")];
      const layout = layoutFor(columns, physicalNames, carriers, entry.carrier, `matrix-${index}`);
      const descriptor = readDescriptor(new PostcardReader(Uint8Array.from(layout.rootDescriptor)));
      const raw = createRecord(descriptor, [
        uuidBytes(ROOT_ID),
        ...columns.map((source, slot) => {
          const encoded = encodeNativeRowValues(
            [
              carriers[slot] === "CurrentRow"
                ? { ...source, nullable: true, sparse: undefined }
                : logicalStorageColumns([source])[0]!,
            ],
            [{ type: "Text", value: `slot-${slot}` }],
          );
          return carriers[slot] === "CurrentRow" && source.nullable
            ? Uint8Array.from([1, ...encoded])
            : encoded;
        }),
        uuidBytes("00000000-0000-4000-8000-000000000099"),
      ]);
      const expectedValues = columns.map((_, slot) => `slot-${slot}`);
      expect(layout.publicFields.map((field) => field.name)).toEqual(
        columns.map((column) => column.name),
      );
      expect(layout.publicFields.map((field) => field.descriptorFieldName)).toEqual(physicalNames);
      expect(descriptor.slice(1, -1).map((field) => field.name)).toEqual(physicalNames);
      const manager = new SubscriptionManager<{
        id: string;
        values: string[];
        names: string[];
      }>();
      const delta = emptyDelta([layout]);
      // Terminal occurrence keys use their own stable row-key arm, independent
      // of the ValueType enum's shifted UUID discriminant.
      const key = [10, ...uuidBytes(ROOT_ID)];
      delta.terminalOperations = [
        {
          rootLayoutId: layout.id,
          root_key: key,
          path: [],
          edit: { Insert: { index: 0, key, value: [...raw] } },
        },
      ];
      expect(
        manager.handleDelta(
          delta,
          (row) => ({
            id: row.id,
            values: row.values.map((value) => (value as { type: "Text"; value: string }).value),
            names: [
              ...(row as WasmRow & { valuesByColumn: Map<string, Value> }).valuesByColumn.keys(),
            ],
          }),
          columns,
        ).all,
        JSON.stringify(entry),
      ).toEqual([
        { id: ROOT_ID, values: expectedValues, names: columns.map((column) => column.name) },
      ]);
      expect(descriptor).toHaveLength(columns.length + 2); // key + public slots + hidden provenance
    }
  });

  it("decodes mixed CurrentRow/Logical public physical slots and their public names", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "count", column_type: { type: "Integer" }, nullable: false, sparse: true },
    ];
    const descriptor: DescriptorField[] = [
      { name: "row_uuid", valueType: { tag: 11 } },
      { name: "user_title", valueType: { tag: 15, inner: { tag: 8 } } },
      { name: "user_count", valueType: { tag: 4 } },
      { name: "__route_provenance", valueType: { tag: 11 } },
    ];
    const layout: NativeTerminalRootLayout = {
      id: "mixed-root-and-child-paths",
      rootDescriptor: encodedDescriptor(descriptor),
      rootKeySlot: 0,
      rootKeyFieldName: "row_uuid",
      carrier: "Logical",
      publicFields: [
        { name: "title", descriptorFieldName: "user_title", slot: 1, carrier: "CurrentRow" },
        { name: "count", descriptorFieldName: "user_count", slot: 2, carrier: "Logical" },
      ],
    };
    const raw = createRecord(descriptor, [
      uuidBytes(ROOT_ID),
      encodeNativeRowValues([{ ...columns[0], nullable: true }], [{ type: "Text", value: "root" }]),
      encodeNativeRowValues(logicalStorageColumns([columns[1]]), [{ type: "Integer", value: 7 }]),
      uuidBytes("00000000-0000-4000-8000-000000000099"),
    ]);
    const decode = compileNativeTerminalRootDecoder(layout, descriptor, columns);
    expect(decode(ROOT_ID, raw).values).toEqual([
      { type: "Text", value: "root" },
      { type: "Integer", value: 7 },
    ]);
  });

  it("is sensitive to explicit-projection aliases and logical include slot identities", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "forward_0", column_type: { type: "Text" }, nullable: false },
    ];
    const layout = layoutFor(
      columns,
      ["projected_title", "forward_0"],
      ["CurrentRow", "Logical"],
      "CurrentRow",
      "projection-include-plant",
    );
    const descriptor = readDescriptor(new PostcardReader(Uint8Array.from(layout.rootDescriptor)));
    expect(() => compileNativeTerminalRootDecoder(layout, descriptor, columns)).not.toThrow();
    const planted = {
      ...layout,
      publicFields: [
        { ...layout.publicFields[0]!, descriptorFieldName: "user_title" },
        { ...layout.publicFields[1]!, descriptorFieldName: "user_forward_0" },
      ],
    };
    expect(() => compileNativeTerminalRootDecoder(planted, descriptor, columns)).toThrow(
      /terminal root layout does not match/,
    );
  });

  it("rejects every one-dimension registration mutation and immutable layout redefinition", () => {
    const columns: ColumnDescriptor[] = [
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ];
    const valid = layoutFor(columns, ["user_title"], ["Logical"], "Logical", "immutable-layout");
    const mutations: Array<[string, NativeTerminalRootLayout]> = [
      [
        "wrong layout carrier",
        {
          ...valid,
          carrier: "CurrentRow",
          publicFields: [{ ...valid.publicFields[0]!, carrier: "CurrentRow" }],
        },
      ],
      ["wrong root key slot", { ...valid, rootKeySlot: 1 }],
      [
        "renamed physical slot",
        { ...valid, publicFields: [{ ...valid.publicFields[0]!, descriptorFieldName: "renamed" }] },
      ],
      ["missing slot", { ...valid, publicFields: [{ ...valid.publicFields[0]!, slot: 99 }] }],
      [
        "duplicate slot",
        {
          ...valid,
          publicFields: [valid.publicFields[0]!, { ...valid.publicFields[0]!, name: "other" }],
        },
      ],
      [
        "wrong public name",
        { ...valid, publicFields: [{ ...valid.publicFields[0]!, name: "wrong" }] },
      ],
      [
        "wrong type",
        {
          ...valid,
          rootDescriptor: encodedDescriptor([
            { name: "row_uuid", valueType: { tag: 11 } },
            { name: "user_title", valueType: { tag: 4 } },
            { name: "__route_provenance", valueType: { tag: 11 } },
          ]),
        },
      ],
    ];
    for (const [name, mutated] of mutations) {
      const manager = new SubscriptionManager<{ id: string }>();
      expect(
        () => manager.handleDelta(emptyDelta([mutated]), (row) => ({ id: row.id }), columns),
        name,
      ).toThrow(/terminal root layout/);
    }
    const manager = new SubscriptionManager<{ id: string }>();
    manager.handleDelta(emptyDelta([valid]), (row) => ({ id: row.id }), columns);
    expect(() =>
      manager.handleDelta(
        emptyDelta([{ ...valid, rootKeyFieldName: "other" }]),
        (row) => ({ id: row.id }),
        columns,
      ),
    ).toThrow(/redefined/);
  });
});
