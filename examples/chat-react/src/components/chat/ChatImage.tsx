import { useEffect, useMemo, useState } from "react";
import { useDb } from "jazz-tools/react";
import { DownloadIcon, ImageOffIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { downloadUrl } from "@/lib/utils";
import { app, type Attachment } from "../../../schema.js";

interface ChatImageProps {
  attachment: Attachment;
}

export function ChatImage({ attachment }: ChatImageProps) {
  const db = useDb();
  const [imageUrl, setImageUrl] = useState<string | null>(null);
  const [status, setStatus] = useState<"loading" | "loaded" | "error">("loading");
  const serverUrl = db.getConfig().serverUrl;
  const fileReadOptions = useMemo(
    () => (serverUrl ? ({ tier: "edge" as const } as const) : undefined),
    [serverUrl],
  );

  useEffect(() => {
    let isActive = true;
    let objectUrl: string | null = null;
    setStatus("loading");
    setImageUrl(null);

    async function loadImage() {
      let blob: Blob;
      try {
        blob = await db.loadFileAsBlob(app, attachment.fileId, fileReadOptions);
      } catch {
        if (isActive) {
          setStatus("error");
        }
        return;
      }
      if (!isActive) return;

      objectUrl = URL.createObjectURL(blob);
      setImageUrl(objectUrl);
      setStatus("loaded");
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

  if (status === "error") {
    return (
      <div
        role="img"
        aria-label={attachment.name}
        className="mt-1 flex max-w-xs items-center gap-2 rounded-md border border-dashed p-3 text-muted-foreground"
      >
        <ImageOffIcon className="size-4 shrink-0" />
        <span className="text-sm wrap-anywhere">{attachment.name}</span>
      </div>
    );
  }

  return (
    <div className="group relative mt-1 max-w-xs">
      {imageUrl ? (
        <img src={imageUrl} alt={attachment.name} className="rounded-md max-h-64 object-contain" />
      ) : (
        <div
          className="h-32 w-full animate-pulse rounded-md bg-muted"
          aria-label={`Loading ${attachment.name}`}
        />
      )}
      {status === "loaded" && (
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
      )}
    </div>
  );
}
