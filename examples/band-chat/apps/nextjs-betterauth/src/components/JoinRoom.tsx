"use client";

import { useAll, useDb } from "jazz-tools/react";
import { Button, Center, EmptyState, HStack, MobileNavToggle, VStack } from "@astryxdesign/core";
import { app, type Profile } from "../../schema";

/**
 * Shown for a room link you are not a member of. You cannot read the room yet,
 * so all you can do is ask: the creator sees your profile and decides.
 */
export function JoinRoom({
  roomId,
  profile,
  onDismiss,
}: {
  roomId: string;
  profile: Profile;
  onDismiss: () => void;
}) {
  const db = useDb();
  const { data: requests } = useAll(
    app.joinRequests.where({ roomId, requester: profile.author }),
  );
  const pending = requests?.[0];

  return (
    <VStack height="100%">
      <HStack paddingInline={4} paddingBlock={2}>
        <MobileNavToggle />
      </HStack>
      <Center axis="both" padding={4} className="page-fill">
        {pending ? (
          <EmptyState
            headingLevel={1}
            title="Waiting for the room creator"
            description="Your request to join has been sent. The room appears in your list as soon as you are admitted."
            actions={
              <HStack gap={2}>
                <Button
                  label="Cancel request"
                  onClick={() => db.delete(app.joinRequests, pending.id)}
                />
                <Button label="Close" variant="ghost" onClick={onDismiss} />
              </HStack>
            }
          />
        ) : (
          <EmptyState
            headingLevel={1}
            title="You have a room link"
            description="Ask to join and the room creator will see your name and photo. Nothing in the room is visible until they admit you."
            actions={
              <HStack gap={2}>
                <Button
                  label="Ask to join"
                  variant="primary"
                  isDisabled={!requests}
                  onClick={() =>
                    db.insert(app.joinRequests, {
                      roomId,
                      requester: profile.author,
                      profileId: profile.id,
                    })
                  }
                />
                <Button label="Not now" variant="ghost" onClick={onDismiss} />
              </HStack>
            }
          />
        )}
      </Center>
    </VStack>
  );
}
