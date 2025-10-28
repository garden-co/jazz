import { Account, FileStream } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { UploadedFile } from "./schema";
export function DownloaderPeer(props: { testCoMapId: string }) {
  const account = useAccount();
  const testCoMap = useCoState(UploadedFile, props.testCoMapId, {});
  const [synced, setSynced] = useState(false);

  useEffect(() => {
    if (!account.me) {
      return;
    }

    async function run(me: Account, uploadedFileId: string) {
      const uploadedFile = await UploadedFile.load(uploadedFileId, {
        loadAs: me,
      });

      if (!uploadedFile) {
        throw new Error("Uploaded file not found");
      }

      me.$jazz.waitForAllCoValuesSync().then(() => {
        setSynced(true);
      });

      uploadedFile.$jazz.set("coMapDownloaded", true);

      await FileStream.loadAsBlob(uploadedFile.$jazz.refs.file!.id, {
        loadAs: me,
      });

      uploadedFile.$jazz.set("syncCompleted", true);
    }

    run(account.me, props.testCoMapId);
  }, []);

  return (
    <>
      <h1>Downloader Peer</h1>
      <div>Fetching: {props.testCoMapId}</div>
      <div>Synced: {String(synced)}</div>
      <div data-testid="result">
        Covalue:{" "}
        {Boolean(testCoMap?.$jazz.id) ? "Downloaded" : "Not Downloaded"}
      </div>
      <div data-testid="result">
        File:{" "}
        {Boolean(testCoMap?.syncCompleted) ? "Downloaded" : "Not Downloaded"}
      </div>
    </>
  );
}
