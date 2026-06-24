import { useState } from "react";
import { QRCodeSVG } from "qrcode.react";
import { Check, Copy, Share2Icon } from "lucide-react";
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

interface ShareModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  chatId: string;
  joinCode?: string;
}

export const ShareModal = ({ open, onOpenChange, chatId, joinCode }: ShareModalProps) => {
  const [copied, setCopied] = useState(false);

  const inviteLink = joinCode
    ? `${window.location.origin}/#/invite/${chatId}/${joinCode}`
    : `${window.location.origin}/#/chat/${chatId}`;

  const handleCopy = async () => {
    await navigator.clipboard.writeText(inviteLink);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleShare = async () => {
    if (navigator.share) {
      try {
        await navigator.share({
          title: "Join my chat",
          text: "I'm inviting you to a chat.",
          url: inviteLink,
        });
      } catch (err) {
        console.error("Error sharing:", err);
      }
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Invite to chat</DialogTitle>
        </DialogHeader>

        <DialogDescription>
          Share the link below with your friends to invite them to join the chat.
        </DialogDescription>

        <div className="flex flex-col gap-4 py-4 items-center">
          <QRCodeSVG value={inviteLink} size={192} />

          <div className="flex w-full flex-col gap-2">
            <div className="flex w-full items-center gap-2">
              <Input id="link" aria-label="Invite link" defaultValue={inviteLink} readOnly />
              <Button size="icon" variant="outline" onClick={handleCopy}>
                <span className="sr-only">Copy</span>
                {copied ? <Check /> : <Copy />}
              </Button>
            </div>

            {"share" in navigator && (
              <Button variant="outline" className="w-full gap-2" onClick={handleShare}>
                <Share2Icon className="h-4 w-4" />
                Share Link
              </Button>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button onClick={() => onOpenChange(false)}>Done</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
