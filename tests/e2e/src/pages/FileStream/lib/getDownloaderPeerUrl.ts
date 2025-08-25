import { Loaded } from "jazz-tools";
import { UploadedFile } from "../schema";

export function getDownloaderPeerUrl(value: Loaded<typeof UploadedFile>) {
  const url = new URL(window.location.href);
  url.searchParams.set("valueId", value.$jazz.id);
  return url.toString();
}
