import { schema as s, table } from "jazz-tools";

table("retry_tasks", {
  title: s.string(),
});

if (process.env.JAZZ_SCHEMA_LOADER_FAIL_RETRY === "1") {
  throw new Error("intentional schema fixture failure");
}
