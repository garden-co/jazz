import { useEffect, useState } from "react";
import * as Y from "yjs";
import { useDb, useSession } from "jazz-tools/react";
import { JazzYjsDocumentController } from "./JazzYjsDocumentController.js";

type UseJazzYjsDocumentArgs = {
  roomId: string | null;
};

export function useJazzYjsDocument({ roomId }: UseJazzYjsDocumentArgs): {
  ydoc: Y.Doc;
  isReady: boolean;
} {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [ydoc] = useState(() => new Y.Doc());
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    setIsReady(false);
    if (!roomId || !sessionUserId) return;

    // The doc is reused across rooms, and applyUpdate merges rather than
    // replaces — so clear any previous room's text before the new room
    // bootstraps onto it.
    const text = ydoc.getText("monaco");
    ydoc.transact(() => text.delete(0, text.length), { provider: "jazz" });

    const controller = new JazzYjsDocumentController(db, roomId, sessionUserId, ydoc, () =>
      setIsReady(true),
    );
    return () => controller.destroy();
  }, [db, roomId, sessionUserId, ydoc]);

  return { ydoc, isReady };
}
