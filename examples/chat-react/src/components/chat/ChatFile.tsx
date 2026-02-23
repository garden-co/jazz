import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { dataUriToBlob, downloadBlob, formatBytes } from "@/lib/utils";
import type { Attachment } from "../../../schema/app.js";

interface ChatFileProps {
  attachment: Attachment;
}

export function ChatFile({ attachment }: ChatFileProps) {
  const handleDownload = () => {
    downloadBlob(dataUriToBlob(attachment.data, attachment.mimeType), attachment.name);
  };

  return (
    <div className="rounded-xl my-2 flex flex-col">
      <span className="mb-2 wrap-anywhere">{attachment.name}</span>
      <Button variant="secondary" onClick={handleDownload}>
        <DownloadIcon />
        Download{" "}
        {attachment.size > 0 && <span className="text-sm">({formatBytes(attachment.size)})</span>}
      </Button>
    </div>
  );
}
