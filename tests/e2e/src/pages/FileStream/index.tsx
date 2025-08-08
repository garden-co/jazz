import { DownloaderPeer } from "./DownloaderPeer";
import { getValueId } from "./lib/searchParams";
import { UploaderPeer } from "./UploaderPeer";

export function FileStreamTest() {
  const valueId = getValueId();

  if (valueId) {
    return <DownloaderPeer testCoMapId={valueId} />;
  }

  return <UploaderPeer />;
}
