import { useState } from "react";
import { CloudUploadIcon } from "lucide-react";
import { toast } from "sonner";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { cn, fileToBase64 } from "@/lib/utils";

export interface AttachmentData {
  type: "image" | "file";
  name: string;
  data: string;
  mimeType: string;
  size: number;
}

interface UploadModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  accept?: string;
  title: string;
  onUpload: (attachment: AttachmentData) => void;
}

export function UploadModal({ open, onOpenChange, accept, title, onUpload }: UploadModalProps) {
  const [isUploading, setIsUploading] = useState(false);

  async function handleFile(file: File) {
    try {
      setIsUploading(true);
      const data = await fileToBase64(file);
      const type = file.type.startsWith("image/") ? "image" : "file";
      onUpload({ type, name: file.name, data, mimeType: file.type, size: file.size });
      toast.success("Upload successful");
      onOpenChange(false);
    } catch (err) {
      console.error(err);
      toast.error("Upload failed");
    } finally {
      setIsUploading(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={!isUploading ? onOpenChange : undefined}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>

        <Label
          className={cn(
            buttonVariants({ variant: "ghost" }),
            "flex flex-col items-center justify-center border-2 border-dashed rounded-xl aspect-video h-auto w-full transition-colors",
            isUploading
              ? "cursor-not-allowed opacity-60"
              : "cursor-pointer hover:text-muted-foreground",
          )}
        >
          <CloudUploadIcon className="w-1/3! h-1/3!" />

          {isUploading && (
            <div className="w-full px-8 text-center">
              <p>Processing…</p>
            </div>
          )}

          <input
            type="file"
            hidden
            disabled={isUploading}
            accept={accept}
            // Expose handler on DOM for browser tests (Radix portal blocks synthetic events)
            ref={(el) => {
              if (el) (el as Record<string, unknown>).__handleFile = handleFile;
            }}
            onChange={(evt) => {
              const file = evt.target.files?.[0];
              if (!file) return;
              evt.currentTarget.value = "";
              handleFile(file);
            }}
          />
        </Label>

        <DialogFooter>
          <Button variant="ghost" disabled={isUploading} onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
