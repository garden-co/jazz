import { createRoot } from "react-dom/client";
import { useEffect, useState, Suspense } from "react";
import { createJazzClient, JazzProvider, useAll } from "jazz-tools/react";
import { app } from "../schema/app.js";

const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
const appId = (import.meta.env.VITE_JAZZ_APP_ID as string) || "policy-bypass-repro";

type Client = Awaited<ReturnType<typeof createJazzClient>>;

function ItemList({ myUserId }: { myUserId: string }) {
  const rows = useAll(app.owned_items);
  if (!rows) return <p>Loading…</p>;

  const others = rows.filter((r) => r.ownerId !== myUserId);
  const buggy = others.length > 0;

  return (
    <>
      <h2>
        useAll() returned {rows.length} row{rows.length !== 1 && "s"}
        {buggy ? (
          <span className="bug"> — BUG: expected 1 (my row only)</span>
        ) : (
          <span className="ok"> — correct</span>
        )}
      </h2>
      <table>
        <thead>
          <tr>
            <th>title</th>
            <th>ownerId</th>
            <th>mine?</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id} className={r.ownerId !== myUserId ? "bug" : ""}>
              <td>{r.title}</td>
              <td>
                <code>{r.ownerId.slice(0, 20)}…</code>
              </td>
              <td>{r.ownerId === myUserId ? "yes" : "no"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p>
        Policy: <code>allowRead.where(&#123; ownerId: session.user_id &#125;)</code>
        <br />
        My user_id: <code>{myUserId.slice(0, 20)}…</code>
        <br />
        Server: <code>{serverUrl ?? "none (local only)"}</code>
        <br />
        {buggy && <>Other users' rows should not be visible.</>}
      </p>
    </>
  );
}

async function seedAndStart(): Promise<Client> {
  // Phase 1: seed other users' rows via separate clients.
  for (const name of ["bob", "carol"]) {
    const other = await createJazzClient({
      appId,
      dbName: `seed-${name}-${Date.now()}`,
      serverUrl,
      localAuthMode: "demo",
      localAuthToken: name,
    });
    const otherId = other.session?.user_id ?? name;
    other.db.insert(app.owned_items, { title: `${name}-item`, ownerId: otherId });
    await new Promise((r) => setTimeout(r, 1500));
    await other.shutdown();
  }

  // Phase 2: connect as alice and insert her own row.
  const alice = await createJazzClient({
    appId,
    dbName: `alice-${Date.now()}`,
    serverUrl,
    localAuthMode: "demo",
    localAuthToken: "alice",
  });
  const aliceId = alice.session?.user_id ?? "unknown";
  alice.db.insert(app.owned_items, { title: "alice-item", ownerId: aliceId });

  // Wait for all data to sync.
  await new Promise((r) => setTimeout(r, 2000));

  return alice;
}

function App() {
  const [client, setClient] = useState<Client | null>(null);

  useEffect(() => {
    seedAndStart().then(setClient);
    return () => {
      client?.shutdown();
    };
  }, []);

  if (!client) return <p>Seeding data…</p>;

  return (
    <JazzProvider client={client}>
      <h1>Policy Bypass Repro</h1>
      <ItemList myUserId={client.session?.user_id ?? "unknown"} />
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <Suspense fallback={<p>Loading…</p>}>
    <App />
  </Suspense>,
);
