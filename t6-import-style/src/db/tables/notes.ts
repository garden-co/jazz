import { schema } from "jazz-tools";
export const notes = schema.table({ text: schema.string(), owner_id: schema.uuid(), boardId: schema.uuid().optional() }, { board: schema.rel("boards", "boardId") });
