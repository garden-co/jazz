import { co, type FileStream, type ID } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import { DownloadIcon } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { downloadBlob, formatBytes } from "@/lib/utils";

interface ChatFileProps {
  fileId: ID<FileStream>;
}

export const ChatFile = ({ fileId }: ChatFileProps) => {
  const file = useCoState(co.fileStream(), fileId);
  if (!file.$isLoaded) return <div>{file.$jazz.loadingState}...</div>;
  const fileName = file.getMetadata()?.fileName || "";
  const fileSize = file.getMetadata()?.totalSizeBytes || 0;

  return (
    <div className="rounded-xl my-2 flex flex-col">
      <span className="mb-2 wrap-anywhere">{fileName}</span>

      <Button
        variant="secondary"
        onClick={async () => {
          const blob = file.toBlob();
          if (!blob) {
            toast.error("File was corrupted");
            return;
          }
          downloadBlob(blob, fileName);
        }}
      >
        <DownloadIcon />
        Download{" "}
        {fileSize && <span className="text-sm">({formatBytes(fileSize)})</span>}
      </Button>
    </div>
  );
};
