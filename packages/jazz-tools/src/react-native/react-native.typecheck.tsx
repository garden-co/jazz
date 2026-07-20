// Typecheck-only port of the deleted origin/main React Native tests to the v2 API.
import type { ReactNode } from "react";
import {
  Db,
  JazzProvider,
  UnimplementedSqliteStorageDriver,
  createDb,
  createJazzClient,
  schema,
  useAll,
  useAllSuspense,
  useDb,
  useJazzClient,
  useLocalFirstAuth,
  useSession,
  type DbConfig,
  type JazzClient,
  type ReactNativeSqliteConnection,
  type ReactNativeSqliteStorageDriver,
} from "./index.js";

const app = schema.defineApp({
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean().optional(),
  }),
});

const sqliteStorage: ReactNativeSqliteStorageDriver = new UnimplementedSqliteStorageDriver();

const config: DbConfig = {
  appId: "rn-typecheck",
  serverUrl: "https://sync.example.test",
  sqliteStorage,
};

async function clientFactory(): Promise<JazzClient> {
  const db = await createDb(config);
  const client = await createJazzClient(config);
  const rows = await db.all(app.todos);
  const one = await db.one(app.todos.where({ title: { eq: "milk" } }));

  rows satisfies Array<{ id: string; title: string; done?: boolean | null }>;
  one satisfies { id: string; title: string; done?: boolean | null } | null;
  db satisfies Db;
  return client;
}

function Hooks({ children }: { children: ReactNode }) {
  const db = useDb();
  const client = useJazzClient();
  const session = useSession();
  const auth = useLocalFirstAuth({
    key: "rn-auth-key",
    authSecretStorageKey: "rn-auth-key-legacy",
    appId: "rn-typecheck",
  });
  const todos = useAll(app.todos);
  const suspenseTodos = useAllSuspense(app.todos);

  db satisfies Db;
  client satisfies { db: Db; shutdown(): Promise<void> };
  session satisfies ReturnType<typeof useSession>;
  auth.secret satisfies string | null;
  todos satisfies ReturnType<typeof useAll<typeof app.todos._rowType>>;
  suspenseTodos satisfies Array<{ id: string; title: string; done?: boolean | null }>;

  return (
    <JazzProvider config={config} fallback={null}>
      {children}
    </JazzProvider>
  );
}

async function storageDriverShape(connection: ReactNativeSqliteConnection) {
  await connection.execute("create table if not exists jazz_kv (key text primary key, value blob)");
  const rows = await connection.query<{ key: string }>("select key from jazz_kv");
  await connection.transaction((tx) => tx.execute("delete from jazz_kv where key = ?", ["k"]));
  await connection.close();
  rows satisfies readonly { key: string }[];
}

void clientFactory;
void Hooks;
void storageDriverShape;
