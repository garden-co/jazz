// A keyless backend's types: `keyless(app)` drops each column the generated
// types mark `encrypted: true` from rows (`?: undefined`), inserts and
// filters; the client's `app` keeps the plaintext types.
import { describe, expect, expectTypeOf, it } from "vitest";
import { keyless, type KeylessApp, type KeylessRow } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/test-app.js";
import type { App, AppTypes, InsertOf, RowOf } from "../../src/typed-app.js";

// The type map of crates/jazz-codegen/tests/apps/encrypted/jazz.generated.ts.
type Task = {
  readonly id: string;
  projectId: string;
  title: string;
  notes: string | null;
  done: boolean;
};
type TaskInsert = { projectId: string; title: string; notes?: string | null; done: boolean };
interface Types extends AppTypes {
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      projectId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string; encrypted: true };
      notes: { kind: "string"; nullable: true; value: string; encrypted: true };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
  notes: {
    row: { readonly id: string; body: string };
    insert: { body: string };
    columns: { body: { kind: "string"; nullable: false; value: string } };
    relations: {};
  };
}
declare const generated: App<Types>;

// The same app inline, as a test defines one.
const file = {
  tables: {
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes", done: "boolean" },
      encryption: {
        space: "projectId",
        scope: "tasks",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
  },
} as const;

describe("keyless backend types", () => {
  it("drop a generated app's encrypted columns from rows, inserts and filters", () => {
    const backend = keyless(generated);
    expectTypeOf<RowOf<typeof backend.tasks>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      done: boolean;
      title?: undefined;
      notes?: undefined;
    }>();
    expectTypeOf<InsertOf<typeof backend.tasks>>().toEqualTypeOf<{
      projectId: string;
      done: boolean;
    }>();
    backend.tasks.where({ done: true });
    // @ts-expect-error the backend can't filter by an encrypted column
    backend.tasks.where({ title: "x" });
    // The client's app keeps the plaintext types.
    expectTypeOf<RowOf<typeof generated.tasks>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      title: string;
      notes: string | null;
      done: boolean;
    }>();
    generated.tasks.where({ title: "x" });
  });

  it("leave tables without encrypted columns, and apps without encryption, as they are", () => {
    expectTypeOf<RowOf<KeylessApp<typeof generated>["notes"]>>().toEqualTypeOf<
      RowOf<typeof generated.notes>
    >();
    const plain = testApp({ tables: { notes: { columns: { body: "string" } } } } as const);
    expectTypeOf<KeylessApp<typeof plain>>().toEqualTypeOf<typeof plain>();
  });

  it("type an inline encrypted app as its bindings do", () => {
    const app = testApp(file);
    expectTypeOf<RowOf<typeof app.tasks>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      title: string;
      done: boolean;
    }>();
    expectTypeOf<RowOf<KeylessApp<typeof app>["tasks"]>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      done: boolean;
      title?: undefined;
    }>();
  });

  it("KeylessRow leaves a row with no encrypted column unchanged", () => {
    expectTypeOf<KeylessRow<Task, never>>().toEqualTypeOf<Task>();
    expectTypeOf<KeylessRow<Task, "title">>().toEqualTypeOf<{
      readonly id: string;
      projectId: string;
      notes: string | null;
      done: boolean;
      title?: undefined;
    }>();
  });

  it("is the app itself at runtime", () => {
    const app = testApp(file);
    expect(keyless(app)).toBe(app);
  });
});
