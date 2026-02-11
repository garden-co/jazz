import type { FileStream, ID, ImageDefinition } from "jazz-tools";
import { useEffect, useState } from "react";
import { loadImage } from "jazz-tools/media";
import { Image } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { downloadBlob, formatBytes } from "@/lib/utils";

interface ChatImageProps {
  imageId: ID<ImageDefinition>;
}

interface ImageType {
  height: number;
  width: number;
  image: FileStream;
}

export const ChatImage = ({ imageId }: ChatImageProps) => {
  const [image, setImage] = useState<ImageType>();

  // Lazy load the image when the component mounts.
  // This step is unnecessary for normal use, but in order for us
  // to evaluate and show the file size, for downloading,
  // we need to load the image. We don't want to block the image render
  // so we do this in a useEffect.
  useEffect(() => {
    loadImage(imageId).then((image) => {
      if (image) setImage(image);
    });
  }, [imageId]);

  return (
    <div className="text-foreground rounded-xl my-2 flex flex-col">
      <div className="rounded-xl overflow-hidden mb-2">
        <Image imageId={imageId} className="max-w-[50vw]" height="original" />
      </div>
      <Button
        variant="secondary"
        onClick={async () => {
          const img = await loadImage(imageId);
          if (!img) {
            toast.error("Could not load image");
            return;
          }
          const blob = img.image.toBlob();
          if (!blob) {
            toast.error("File was corrupted");
            return;
          }
          const metadata = img.image.getMetadata();
          const ext = metadata?.mimeType?.split("/")[1] || "png";
          downloadBlob(blob, `image-${imageId}.${ext}`);
        }}
      >
        <DownloadIcon />
        Download{" "}
        {image &&
          `(${formatBytes(image.image.getMetadata()?.totalSizeBytes || 0)})`}
      </Button>
    </div>
  );
};
