import { useEffect, useState } from "react";
import { useDb } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { downloadUrl } from "@/lib/utils";
import { app, type Attachment } from "../../../schema/app.js";

interface ChatImageProps {
  attachment: Attachment;
}

export function ChatImage({ attachment }: ChatImageProps) {
  const db = useDb();
  const [imageUrl, setImageUrl] = useState<string | null>(null);

  useEffect(() => {
    let isActive = true;
    let objectUrl: string | null = null;

    void db.loadFileAsBlob(app, attachment.fileId).then((blob) => {
      if (!isActive) {
        return;
      }

      objectUrl = URL.createObjectURL(blob);
      setImageUrl(objectUrl);
    });

    return () => {
      isActive = false;
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
    };
  }, [attachment.fileId, db]);

  const handleDownload = async () => {
    if (!imageUrl) return;
    downloadUrl(imageUrl, attachment.name);
  };

  return (
    <div className="group relative mt-1 max-w-xs">
      {imageUrl ? (
        <img src={imageUrl} alt={attachment.name} className="rounded-md max-h-64 object-contain" />
      ) : null}
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
