// Transforms that return null: an alpha.64 transform on an optional column
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
