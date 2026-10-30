import * as React from "react";
import { useDb } from "jazz-tools/react";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { redeemInvite, type Invite } from "../sharing.js";

interface JoinDialogProps {
  invite: Invite | undefined;
  userId: string | undefined;
  onJoined: (folderId: string) => void;
  onClose: () => void;
}

export function JoinDialog({ invite, userId, onJoined, onClose }: JoinDialogProps) {
  const db = useDb();
  const [error, setError] = React.useState<string>();
  React.useEffect(() => setError(undefined), [invite?.code]);
  const access = invite?.role === "editor" ? "view and edit" : "view";
  return (
    <Dialog isOpen={invite !== undefined} onOpenChange={(open) => !open && onClose()} width={440}>
      <VStack gap={4}>
        <DialogHeader title="Join a shared folder" onOpenChange={(open) => !open && onClose()} />
        <Text>Someone invited you to {access} a folder. It will appear under Shared with me.</Text>
        {error && (
          <Banner
            status="error"
            title="This invite could not be used"
            description="It may have been revoked, or you may be offline. Ask for a new link."
          />
        )}
        <HStack gap={2} hAlign="end">
          <Button label="Not now" variant="secondary" onClick={onClose} />
          <Button
            label="Join folder"
            variant="primary"
            isDisabled={!userId}
            clickAction={async () => {
              if (!invite || !userId) return;
              try {
                await redeemInvite(db, invite, userId);
                onJoined(invite.folderId);
              } catch (cause) {
                setError(String(cause));
              }
            }}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
