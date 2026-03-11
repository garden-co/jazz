import { table, col } from "jazz-tools";

table("files", {
  name: col.string(),
  mimeType: col.string(),
  parts: col.array(col.ref("file_parts")),
  partSizes: col.array(col.int()),
});

table("file_parts", {
  data: col.bytes(),
});

table("uploads", {
  size: col.int(),
  last_modified: col.timestamp(),
  file_id: col.ref("files"),
  owner_id: col.string(),
});
