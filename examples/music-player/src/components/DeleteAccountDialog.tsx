import { MusicaAccount } from "@/1_schema";
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
import { Label } from "@/components/ui/label";
import { useSuspenseAccount } from "jazz-tools/react-core";
import { useEffect, useMemo, useState } from "react";

const CONFIRMATION_PHRASE = "I want to delete my account";

export interface DeleteAccountDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm?: () => Promise<void> | void;
}

export function DeleteAccountDialog({
  isOpen,
  onOpenChange,
  onConfirm,
}: DeleteAccountDialogProps) {
  const [confirmationText, setConfirmationText] = useState("");
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const profileName = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.profile.name,
  });

  useEffect(() => {
    if (!isOpen) {
      setConfirmationText("");
      setIsPending(false);
      setError(null);
    }
  }, [isOpen]);

  const isPhraseMatch = useMemo(
    () => confirmationText === CONFIRMATION_PHRASE,
    [confirmationText],
  );

  const canDelete = isPhraseMatch && !isPending;

  async function handleConfirm() {
    if (!canDelete) return;

    setIsPending(true);
    setError(null);
    try {
      await onConfirm?.();
    } catch (e) {
      console.error("Delete account failed:", e);
      setError("Failed to delete your account. Please try again.");
      setIsPending(false);
      return;
    }
    onOpenChange(false);
  }

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete account</DialogTitle>
          <DialogDescription>
            This will delete your music-player data and sign you out.
          </DialogDescription>
        </DialogHeader>

        <div className="text-sm text-gray-700">
          You are deleting data for:{" "}
          <span className="font-medium">{profileName}</span>
        </div>

        <div className="space-y-2">
          <Label htmlFor="delete-account-confirmation">
            Type the phrase to confirm
          </Label>
          <div className="text-sm text-gray-600">
            <span className="font-mono">{CONFIRMATION_PHRASE}</span>
          </div>
          <Input
            id="delete-account-confirmation"
            value={confirmationText}
            onChange={(e) => {
              setConfirmationText(e.target.value);
              if (error) setError(null);
            }}
            disabled={isPending}
            autoComplete="off"
            spellCheck={false}
            autoCapitalize="none"
            autoCorrect="off"
            placeholder={CONFIRMATION_PHRASE}
          />
          {error && <p className="text-sm text-red-600">{error}</p>}
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={isPending}
          >
            Cancel
          </Button>
          <Button
            variant="destructive"
            onClick={handleConfirm}
            disabled={!canDelete}
          >
            {isPending ? "Deleting…" : "Delete account"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
