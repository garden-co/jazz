import { FileNotFoundError, IncompleteFileDataError } from "jazz-tools";
import { useEffect, useState } from "react";
import { useDb } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { downloadUrl } from "@/lib/utils";
import { app, type Attachment } from "../../../schema.js";

const IMAGE_LOAD_RETRY_DELAY_MS = 150;
const MAX_IMAGE_LOAD_RETRIES = 20;

function isRetryableImageLoadError(error: unknown) {
  return error instanceof FileNotFoundError || error instanceof IncompleteFileDataError;
}

interface ChatImageProps {
  attachment: Attachment;
}

export function ChatImage({ attachment }: ChatImageProps) {
  const db = useDb();
  const [imageUrl, setImageUrl] = useState<string | null>(null);

  useEffect(() => {
    let isActive = true;
    let objectUrl: string | null = null;
    let retryTimer: number | null = null;
    let retries = 0;

    async function loadImage() {
      try {
        const blob = await db.loadFileAsBlob(app, attachment.fileId, { tier: "edge" });
        if (!isActive) return;

        objectUrl = URL.createObjectURL(blob);
        setImageUrl(objectUrl);
      } catch (error) {
        if (!isActive) return;

        if (isRetryableImageLoadError(error) && retries < MAX_IMAGE_LOAD_RETRIES) {
          retries += 1;
          retryTimer = window.setTimeout(() => {
            retryTimer = null;
            void loadImage();
          }, IMAGE_LOAD_RETRY_DELAY_MS);
          return;
        }

        console.error(error);
      }
    }

    void loadImage();

    return () => {
      isActive = false;
      if (retryTimer !== null) {
        window.clearTimeout(retryTimer);
      }
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
    };
  }, [attachment.fileId, db]);

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
