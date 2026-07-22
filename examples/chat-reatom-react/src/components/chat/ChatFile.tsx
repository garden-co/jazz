import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { jazz } from "@/jazz";
import { downloadBlob, formatBytes } from "@/lib/utils";
import { app, type Attachment } from "../../../schema.js";

interface ChatFileProps {
  attachment: Attachment;
}

export const ChatFile = reatomComponent(({ attachment }: ChatFileProps) => {
  const { db } = jazz();
  const fileReadOptions = db.getConfig().serverUrl ? { tier: "edge" as const } : undefined;

  const handleDownload = async () => {
    const blob = await wrap(db.loadFileAsBlob(app, attachment.fileId, fileReadOptions));
    downloadBlob(blob, attachment.name);
  };

  return (
    <div className="rounded-xl my-2 flex flex-col">
      <span className="mb-2 wrap-anywhere">{attachment.name}</span>
      <Button
        variant="secondary"
        onClick={async (event) => {
          event.stopPropagation();
          await handleDownload();
        }}
        onPointerDown={(event) => {
          event.stopPropagation();
        }}
      >
        <DownloadIcon />
        Download{" "}
        {attachment.size > 0 && <span className="text-sm">({formatBytes(attachment.size)})</span>}
      </Button>
    </div>
  );
}, "ChatFile");
