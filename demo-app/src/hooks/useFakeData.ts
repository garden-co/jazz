import { useState, useEffect, useRef } from "react";
import type { WasmDatabaseLike } from "@jazz/react";
import type { ObjectId } from "@/generated/types";
import { generateFakeData } from "@/utils/fakeData";

const DEFAULT_ISSUE_COUNT = 50;

export function useFakeData(db: WasmDatabaseLike | null) {
  const [initialized, setInitialized] = useState(false);
  const [currentUserId, setCurrentUserId] = useState<ObjectId | null>(null);
  const initRef = useRef(false);

  useEffect(() => {
    if (!db || initRef.current) return;
    initRef.current = true;

    // Parse URL param for item count
    const params = new URLSearchParams(window.location.search);
    const itemCount = parseInt(params.get("items") || String(DEFAULT_ISSUE_COUNT), 10);

    generateFakeData(db, itemCount).then((userId) => {
      setCurrentUserId(userId);
      setInitialized(true);
    });
  }, [db]);

  return { initialized, currentUserId };
}
