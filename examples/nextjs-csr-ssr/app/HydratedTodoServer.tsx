import { app } from "../schema";
import { db, createServerSnapshot } from "@/lib/jazz-server";
import HydratedTodoClient from "./HydratedTodoClient";

// Use `JazzContext.forSession(session)` to get a prefetch which runs as the
// client to pre-seed data scoped for the user. Be *extremely* cautious to
// avoid caching private data.
export default async function HydratedTodoServer() {
  const builder = createServerSnapshot();
  await builder.prefetch(db, app.todos);
  const snapshot = builder.dehydrate();

  return <HydratedTodoClient snapshot={snapshot} />;
}
