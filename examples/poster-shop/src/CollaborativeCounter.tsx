import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/app.js";

export function CollaborativeCounter() {
  const db = useDb();
  const session = useSession();
  const events = useAll(app.counter_events) ?? [];
  const count = events.length;
  const canIncrement = Boolean(session?.user_id);

  const increment = () => {
    if (!session?.user_id) return;
    db.insert(app.counter_events, {
      actor_id: session.user_id,
      created_at: new Date().toISOString(),
    });
  };

  return (
    <section>
      <h2>Collaborative Counter</h2>
      <p>Shared value across all connected clients.</p>
      <p>
        Count: <strong>{count}</strong>
      </p>
      <button onClick={increment} disabled={!canIncrement}>
        Increment
      </button>
    </section>
  );
}
