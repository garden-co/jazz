import { useState } from "react";

import { PetPost } from "../1_schema";

import { createInviteLink, useAccount } from "jazz-react";
import QRCode from "qrcode";

import { Loaded } from "jazz-tools";
import { Button, useToast } from "../basicComponents";

export function ShareButton({
  petPost,
}: { petPost?: Loaded<typeof PetPost> | null }) {
  const [existingInviteLink, setExistingInviteLink] = useState<string>();
  const { toast } = useToast();
  const { me } = useAccount();

  return (
    petPost &&
    me?.canAdmin(petPost) && (
      <Button
        size="sm"
        className="py-0"
        disabled={!petPost}
        variant="outline"
        onClick={async () => {
          let inviteLink = existingInviteLink;
          if (petPost && !inviteLink) {
            inviteLink = createInviteLink(petPost, "writer");
            setExistingInviteLink(inviteLink);
          }
          if (inviteLink) {
            const qr = await QRCode.toDataURL(inviteLink, {
              errorCorrectionLevel: "L",
            });
            navigator.clipboard.writeText(inviteLink).then(() =>
              toast({
                title: "Copied invite link to clipboard!",
                description: <img src={qr} className="w-20 h-20" />,
              }),
            );
          }
        }}
      >
        Share
      </Button>
    )
  );
}
