import { useEffect, useMemo, useState } from "react";
import { useDb } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { downloadUrl } from "@/lib/utils";
import { app, type Attachment } from "../../../schema.js";

interface ChatImageProps {
  attachment: Attachment;
}

export function ChatImage({ attachment }: ChatImageProps) {
  const db = useDb();
  const [imageUrl, setImageUrl] = useState<string | null>(null);
  const serverUrl = db.getConfig().serverUrl;
  const fileReadOptions = useMemo(
    () => (serverUrl ? ({ tier: "edge" as const } as const) : undefined),
    [serverUrl],
  );

  useEffect(() => {
    let isActive = true;
    let objectUrl: string | null = null;

    async function loadImage() {
      let blob: Blob;
      try {
        blob = await db.loadFileAsBlob(app, attachment.fileId, fileReadOptions);
      } catch {
        if (isActive) {
          setImageUrl(null);
        }
        return;
      }
      if (!isActive) return;

      objectUrl = URL.createObjectURL(blob);
      setImageUrl(objectUrl);
    }

    loadImage();

    return () => {
      isActive = false;
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
    };
  }, [attachment.fileId, db, fileReadOptions]);

  const handleDownload = () => {
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
        onClick={(event) => {
          event.stopPropagation();
          handleDownload();
        }}
        onPointerDown={(event) => {
          event.stopPropagation();
        }}
        title="Download"
      >
        <DownloadIcon />
      </Button>
    </div>
  );
}
