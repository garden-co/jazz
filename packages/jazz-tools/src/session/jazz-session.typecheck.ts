// Typecheck-only: sessions of any client fit a list of unknown sessions.
import type { JazzSession } from "./jazz-session.js";
declare const typed: JazzSession<{ db: { insert(): void } }>;
const sessions: JazzSession<unknown>[] = [typed];
void sessions;
typed.db.insert();
