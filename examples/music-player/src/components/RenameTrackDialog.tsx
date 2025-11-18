import { MusicTrack } from "@/1_schema";
import { updateMusicTrackTitle } from "@/4_actions";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useState } from "react";
import { ConfirmDialog } from "./ConfirmDialog";

interface EditTrackDialogProps {
  track: MusicTrack;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  onDelete: () => void;
}

export function EditTrackDialog({
  track,
  isOpen,
  onOpenChange,
  onDelete,
}: EditTrackDialogProps) {
  const [newTitle, setNewTitle] = useState(track.title);
  const [isDeleteConfirmOpen, setIsDeleteConfirmOpen] = useState(false);

  function handleSave() {
    if (track && newTitle.trim()) {
      updateMusicTrackTitle(track, newTitle.trim());
      onOpenChange(false);
    }
  }

  function handleCancel() {
    setNewTitle(track?.title || "");
    onOpenChange(false);
  }

  function handleDeleteClick() {
    setIsDeleteConfirmOpen(true);
  }

  function handleDeleteConfirm() {
    onDelete();
    onOpenChange(false);
  }

  function handleKeyDown(event: React.KeyboardEvent) {
    if (event.key === "Enter") {
      handleSave();
    } else if (event.key === "Escape") {
      handleCancel();
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Edit Track</DialogTitle>
          <DialogDescription>Edit "{track?.title}".</DialogDescription>
        </DialogHeader>
        <form className="py-4" onSubmit={handleSave}>
          <Input
            value={newTitle}
            onChange={(e) => setNewTitle(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Enter track name..."
            autoFocus
          />
        </form>
        <DialogFooter className="flex justify-between">
          <Button
            variant="destructive"
            onClick={handleDeleteClick}
            className="mr-auto"
          >
            Delete Track
          </Button>
          <div className="flex gap-2">
            <Button variant="outline" onClick={handleCancel}>
              Cancel
            </Button>
            <Button onClick={handleSave} disabled={!newTitle.trim()}>
              Save
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
      <ConfirmDialog
        isOpen={isDeleteConfirmOpen}
        onOpenChange={setIsDeleteConfirmOpen}
        title="Delete Track"
        description={`Are you sure you want to delete "${track.title}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        onConfirm={handleDeleteConfirm}
        variant="destructive"
      />
    </Dialog>
  );
}
