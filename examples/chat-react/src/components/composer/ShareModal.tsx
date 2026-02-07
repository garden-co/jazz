import type { ID } from "jazz-tools";
import type { Chat } from "@/schema";
import { useState } from "react";
import { ReactQRCode } from "@lglab/react-qr-code";
import { Check, Copy, Share2Icon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle, // Added for accessibility
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";

interface ShareModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  chatId: ID<typeof Chat>;
  inviteLink: string;
}

export const ShareModal = ({
  open,
  onOpenChange,
  inviteLink,
}: ShareModalProps) => {
  const [copied, setCopied] = useState(false);
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
          text: "I'm inviting you to a secure chat on Jazz.",
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
          Share the link below with your friends to invite them to join the
          chat, or have them scan the QR code to join automatically.
        </DialogDescription>
        {inviteLink ? (
          <div className="flex flex-col gap-4 py-4 items-center">
            <ReactQRCode
              value={inviteLink}
              level="Q"
              imageSettings={{
                src: "/jazz-logo.svg",
                width: 64,
                height: 64,
                excavate: true,
              }}
              marginSize={2}
              size={256}
            />
            <div className="flex w-full flex-col gap-2">
              <div className="flex w-full items-center gap-2">
                <Input id="link" defaultValue={inviteLink} readOnly />
                <Button size="icon" variant="outline" onClick={handleCopy}>
                  <span className="sr-only">Copy</span>
                  {copied ? <Check /> : <Copy />}
                </Button>
              </div>

              {"share" in navigator && (
                <Button
                  variant="outline"
                  className="w-full gap-2"
                  onClick={handleShare}
                >
                  <Share2Icon className="h-4 w-4" />
                  Share Link
                </Button>
              )}
            </div>
          </div>
        ) : (
          <div className="py-4 text-center">
            <p>Preparing invite...</p>
          </div>
        )}

        <DialogFooter>
          <Button onClick={() => onOpenChange(false)}>Done</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
