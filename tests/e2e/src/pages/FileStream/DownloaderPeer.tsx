import { useAccount, useCoState } from "jazz-react";
import { Account, FileStream } from "jazz-tools";
import { useEffect, useState } from "react";
import { UploadedFile } from "./schema";
export function DownloaderPeer(props: { testCoMapId: string }) {
  const account = useAccount();
  const testCoMap = useCoState(UploadedFile, props.testCoMapId, {});
  const [synced, setSynced] = useState(false);

  useEffect(() => {
    async function run(me: Account | null | undefined, uploadedFileId: string) {
      const uploadedFile = await UploadedFile.load(uploadedFileId, {
        loadAs: me ?? undefined,
      });

      if (!uploadedFile) {
        throw new Error("Uploaded file not found");
      }

      me?.waitForAllCoValuesSync().then(() => {
        setSynced(true);
      });

      uploadedFile.coMapDownloaded = true;

      await FileStream.loadAsBlob(uploadedFile._refs.file!.id, {
        loadAs: me ?? undefined,
      });

      uploadedFile.syncCompleted = true;
    }

    run(account.me, props.testCoMapId);
  }, []);

  return (
    <>
      <h1>Downloader Peer</h1>
      <div>Fetching: {props.testCoMapId}</div>
      <div>Synced: {String(synced)}</div>
      <div data-testid="result">
        Covalue: {Boolean(testCoMap?.id) ? "Downloaded" : "Not Downloaded"}
      </div>
      <div data-testid="result">
        File:{" "}
        {Boolean(testCoMap?.syncCompleted) ? "Downloaded" : "Not Downloaded"}
      </div>
    </>
  );
}
