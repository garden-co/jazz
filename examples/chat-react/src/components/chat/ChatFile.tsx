import { useDb } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { downloadBlob, formatBytes } from "@/lib/utils";
import { app, type Attachment } from "../../../schema.js";

interface ChatFileProps {
  attachment: Attachment;
}

export function ChatFile({ attachment }: ChatFileProps) {
  const db = useDb();

  const handleDownload = async () => {
    const blob = await db.loadFileAsBlob(app, attachment.fileId, { tier: "edge" });
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
}
