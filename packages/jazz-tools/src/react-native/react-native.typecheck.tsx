// Typecheck-only port of the deleted origin/main React Native tests to the v2 API.
import type { AccountHandle } from "../accounts/state.js";
import type { ReactNode } from "react";
import {
  Db,
  JazzProvider,
  createDb,
  schema,
  useAll,
  useAllSuspense,
  useDb,
  useJazzClient,
  useLocalFirstAuth,
  useSession,
  type AuthSecretStore,
  type DbConfig,
  type JazzClient,
} from "./index.js";
import { openJazzClient, type OpenJazzClientConfig } from "./create-jazz-client.js";

const app = schema.defineApp(
  {
    todos: schema.table(
      {
        title: schema.string(),
        done: schema.boolean().optional(),
      },
      {},
    ),
  },
  { migrations: "none" },
);

declare const authSecretStore: AuthSecretStore;
declare const account: AccountHandle;

const config: DbConfig = {
  appId: "rn-typecheck",
  account,
  serverUrl: "https://sync.example.test",
};

// Persistent RN admission is intentionally a field on the ordinary public
// client config. It does not expose JSI factories, byte encoders, or a
// JavaScript-selectable durable path.
const admittedConfig: OpenJazzClientConfig = {
  appId: "rn-native-relay-typecheck",
  nativeRelay: { capability: new Uint8Array(32) },
  account,
  serverUrl: "https://sync.example.test",
};

async function clientFactory(): Promise<JazzClient> {
  const db = await createDb(config);
  const client = await openJazzClient(config);
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
    store: authSecretStore,
  });
  const todos = useAll(app.todos);
  const suspenseTodos = useAllSuspense(app.todos);

  db satisfies Db;
  client satisfies { db: Db; shutdown(): Promise<void> };
  session satisfies ReturnType<typeof useSession>;
  if (session) {
    session.user satisfies string | null;
    session.identity.subject satisfies string;
  }
  auth.secret satisfies string | null;
  todos satisfies ReturnType<typeof useAll<typeof app.todos._rowType>>;
  suspenseTodos.data satisfies Array<{ id: string; title: string; done?: boolean | null }>;
  suspenseTodos.db satisfies Db;
  todos.db satisfies Db;

  return (
    <JazzProvider config={config} fallback={null}>
      {children}
    </JazzProvider>
  );
}

void clientFactory;
void Hooks;
void admittedConfig;
