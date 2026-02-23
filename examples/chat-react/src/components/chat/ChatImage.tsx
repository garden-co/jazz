import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { dataUriToBlob, downloadBlob } from "@/lib/utils";
import type { Attachment } from "../../../schema/app.js";

interface ChatImageProps {
  attachment: Attachment;
}

export function ChatImage({ attachment }: ChatImageProps) {
  const handleDownload = () => {
    downloadBlob(dataUriToBlob(attachment.data, attachment.mimeType), attachment.name);
  };

  return (
    <div className="group relative mt-1 max-w-xs">
      <img
        src={attachment.data}
        alt={attachment.name}
        className="rounded-md max-h-64 object-contain"
      />
      <Button
        variant="secondary"
        size="icon-xs"
        className="absolute top-1 right-1 opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={handleDownload}
        title="Download"
      >
        <DownloadIcon />
      </Button>
    </div>
  );
}
