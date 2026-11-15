// What schema.ts exported besides the schema, moved here by `jazz-tools upgrade`.
// The schema itself is jazz.app.json; its types are in jazz.generated.ts.
import type { Message as GeneratedMessage } from "./jazz.generated.js";

export type Message = GeneratedMessage & { $createdAt: Date };
