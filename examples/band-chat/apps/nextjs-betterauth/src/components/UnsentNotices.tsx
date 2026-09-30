"use client";

import {
  createContext,
  useContext,
  useEffect,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { useDb } from "jazz-tools/react";
import { Banner, VStack } from "@astryxdesign/core";
import { UnsentMessages } from "../lib/unsent-messages";

const UnsentMessagesContext = createContext<UnsentMessages | null>(null);

/**
 * Holds rejected messages above the rooms, so their notice outlives a room
 * that closes because the sender lost access to it.
 */
export function UnsentMessagesProvider({ children }: { children: ReactNode }) {
  const db = useDb();
  const [unsent] = useState(() => new UnsentMessages());
  useEffect(
    () =>
      // Rejections that no active `wait` handled arrive here. Other writes
      // are still logged, as Jazz does without a listener.
      db.onMutationError((event) => {
        if (!unsent.reportMutationError(event)) console.error("Write rejected", event);
      }),
    [db, unsent],
  );
  return <UnsentMessagesContext.Provider value={unsent}>{children}</UnsentMessagesContext.Provider>;
}

export function useUnsentMessages(): UnsentMessages {
  const unsent = useContext(UnsentMessagesContext);
  if (!unsent) throw new Error("useUnsentMessages needs an UnsentMessagesProvider");
  return unsent;
}

/** One notice per rejected message, until the person dismisses it. */
export function UnsentNotices({ isMember }: { isMember: (roomId: string) => boolean }) {
  const unsent = useUnsentMessages();
  const notices = useSyncExternalStore(unsent.subscribe, unsent.getSnapshot, unsent.getSnapshot);
  if (notices.length === 0) return null;
  return (
    <VStack gap={0}>
      {notices.map((notice) => (
        <Banner
          key={notice.id}
          status="error"
          container="section"
          title={`Your message to ${notice.roomName} wasn't sent`}
          description={[
            isMember(notice.roomId) ? notice.reason : `You were removed from ${notice.roomName}.`,
            notice.text && `Your message: “${notice.text}”`,
            notice.attachmentName && `Attachment: ${notice.attachmentName}`,
          ]
            .filter(Boolean)
            .join(" ")}
          isDismissable
          dismissLabel={`Dismiss unsent message to ${notice.roomName}`}
          onDismiss={() => unsent.dismiss(notice.id)}
        />
      ))}
    </VStack>
  );
}
