import { table, col } from "jazz-tools";

table("messages", {
  author_id: col.string(),
  author_name: col.string(),
  chat_id: col.string(),
  text: col.string(),
  sent_at: col.timestamp(),
});
