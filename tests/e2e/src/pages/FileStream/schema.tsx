import { co, z } from "jazz-tools";

export const UploadedFile = co.map({
  file: co.fileStream(),
  syncCompleted: z.boolean(),
  coMapDownloaded: z.boolean(),
});
