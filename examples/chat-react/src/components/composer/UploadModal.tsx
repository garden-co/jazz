import type { co, Group } from "jazz-tools";
import type { Attachment } from "@/schema";
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
import { Progress } from "@/components/ui/progress";
import { cn, uploadFile } from "@/lib/utils";

interface UploadModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  accept?: string;
  title: string;
  onUpload: (attachment: co.loaded<typeof Attachment>) => void;
  owner?: co.loaded<typeof Group>;
}

export function UploadModal({
  open,
  onOpenChange,
  accept,
  title,
  onUpload,
  owner,
}: UploadModalProps) {
  const [isUploading, setIsUploading] = useState(false);
  const [progress, setProgress] = useState(0);

  async function handleFile(file: File) {
    try {
      setIsUploading(true);
      const uploaded = await uploadFile(file, {
        onProgress: setProgress,
        owner,
      });
      onUpload(uploaded);
      toast.success("Upload successful");
      onOpenChange(false);
    } catch (err) {
      console.error(err);
      toast.error("Upload failed");
    } finally {
      setIsUploading(false);
      setProgress(0);
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
              <p>Uploading…</p>
              <Progress value={progress} className="mt-2" />
            </div>
          )}

          <input
            type="file"
            hidden
            disabled={isUploading}
            accept={accept}
            onChange={(evt) => {
              const file = evt.target.files?.[0];
              if (!file) return;
              evt.currentTarget.value = "";
              handleFile(file);
            }}
          />
        </Label>

        <DialogFooter>
          <Button
            variant="ghost"
            disabled={isUploading}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
