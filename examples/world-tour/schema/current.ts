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

table("bands", {
  name: col.string(),
  logoFileId: col.ref("files").optional(),
});

table("venues", {
  name: col.string(),
  city: col.string(),
  country: col.string(),
  lat: col.float(),
  lng: col.float(),
  capacity: col.int().optional(),
});

table("members", {
  bandId: col.ref("bands"),
  userId: col.string(),
});

table("stops", {
  bandId: col.ref("bands"),
  venueId: col.ref("venues"),
  date: col.timestamp(),
  status: col.enum("confirmed", "tentative", "cancelled"),
  publicDescription: col.string(),
  privateNotes: col.string().optional(),
});
