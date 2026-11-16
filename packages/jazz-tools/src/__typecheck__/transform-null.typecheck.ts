// Transforms that return null: an alpha.65 transform on an optional column
// handled null itself, and `jazz-tools upgrade` moves it unchanged into
// jazz.computed.ts. Jazz never calls a transform with null, so the branch is
// dead, but the callbacks must still satisfy the generated AppTransforms.
import type { TransformImpl } from "../typed-app.js";

interface Transforms {
  isoDate: { stored: Date; view: string };
}

// As upgrade moves it from `s.timestamp().optional().transform({ from, to })`.
const legacy = {
  isoDate: {
    from: (v: Date | null) => (v == null ? null : v.toISOString().slice(0, 10)),
    to: (v: string | null) => (v == null ? null : new Date(`${v}T00:00:00Z`)),
  },
} satisfies TransformImpl<Transforms>;
String(legacy);

// A plain one, as .65 writes it.
const plain = {
  isoDate: { from: (v) => v.toISOString(), to: (v) => new Date(v) },
} satisfies TransformImpl<Transforms>;
String(plain);

const wrongView = {
  isoDate: {
    // @ts-expect-error the view type is still checked: a number isn't a string
    from: (v: Date) => v.getTime(),
    to: (v: string) => new Date(v),
  },
} satisfies TransformImpl<Transforms>;
String(wrongView);

const wrongStored = {
  isoDate: {
    from: (v: Date) => v.toISOString(),
    // @ts-expect-error the stored type is still checked: a string isn't a Date
    to: (v: string) => v,
  },
} satisfies TransformImpl<Transforms>;
String(wrongStored);

// With the app's types, null is allowed only where every column stored
// through the transform is nullable.
import { bindApp, type AppTypes } from "../typed-app.js";

interface NullableTypes extends AppTypes {
  events: {
    row: { readonly id: string; at: string | null; label: string };
    insert: { at?: string | null; label: string };
    columns: {
      at: { kind: "timestamp"; nullable: true; value: Date };
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    transforms: { at: "isoDate"; label: "lowercase" };
  };
}
interface BothTransforms {
  isoDate: { stored: Date; view: string };
  lowercase: { stored: string; view: string };
}

// isoDate stores only a nullable column: the .64 null branch is fine.
const typed = {
  isoDate: legacy.isoDate,
  lowercase: { from: (v: string) => v, to: (v: string) => v.toLowerCase() },
} satisfies TransformImpl<BothTransforms, NullableTypes>;
String(typed);

const nullForRequired = {
  isoDate: legacy.isoDate,
  lowercase: {
    // @ts-expect-error lowercase stores the required `label`: null isn't a value for it
    from: (v: string) => (v === "" ? null : v),
    to: (v: string) => v.toLowerCase(),
  },
} satisfies TransformImpl<BothTransforms, NullableTypes>;
String(nullForRequired);

// bindApp, which the generated module calls with the app's types, checks it
// even where jazz.computed.ts only used the untyped `TransformImpl<Transforms>`.
const loose = {
  isoDate: legacy.isoDate,
  lowercase: {
    from: (v: string) => (v === "" ? null : v),
    to: (v: string) => v.toLowerCase(),
  },
} satisfies TransformImpl<BothTransforms>;
bindApp<NullableTypes, BothTransforms>({
  source: "sha256:00",
  schema: { current: "x", schemas: [], migrations: [] },
  // @ts-expect-error lowercase may return null, but the required `label` is stored through it
  transforms: loose,
});
