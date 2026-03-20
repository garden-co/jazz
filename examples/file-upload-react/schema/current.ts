import { table, col } from "jazz-tools";

table("files", {
  name: col.string(),
  mimeType: col.string(),
  partIds: col.array(col.ref("file_parts")),
  partSizes: col.array(col.int()),
});

table("file_parts", {
  data: col.bytes(),
});

table("uploads", {
  size: col.int(),
  lastModified: col.timestamp(),
  fileId: col.ref("files"),
  ownerId: col.string(),
});
