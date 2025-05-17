import { CoMap, FileStream, coField } from "jazz-tools";

export class UploadedFile extends CoMap {
  file = coField.ref(FileStream);
  syncCompleted = coField.boolean;
  coMapDownloaded = coField.boolean;
}
