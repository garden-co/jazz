import { useState, useEffect, useRef } from "react";
import type { WasmDatabaseLike } from "@jazz/react";
import type { ObjectId, User } from "@/generated/types";
import { app } from "@/generated/client";
import { generateFakeData } from "@/utils/fakeData";

const DEFAULT_ISSUE_COUNT = 50;

export function useFakeData(db: WasmDatabaseLike | null) {
  const [initialized, setInitialized] = useState(false);
  const [currentUserId, setCurrentUserId] = useState<ObjectId | null>(null);
  const initRef = useRef(false);

  useEffect(() => {
    if (!db || initRef.current) return;
    initRef.current = true;

    const params = new URLSearchParams(window.location.search);
    const noFake = params.has("nofake");

    // Check if data already exists by subscribing to users
    let existingUsers: User[] = [];
    const unsubscribe = app.users.subscribeAll(db, (users) => {
      existingUsers = users;
    });

    // Wait longer for sync data when nofake is set
    const waitTime = noFake ? 3000 : 100;

    // Give it a moment to load existing data, then decide
    setTimeout(() => {
      unsubscribe();

      if (existingUsers.length > 0) {
        // Data already exists - use first user as current user
        console.log(`Found ${existingUsers.length} existing users, skipping fake data generation`);
        setCurrentUserId(existingUsers[0].id);
        setInitialized(true);
      } else if (noFake) {
        // nofake mode - don't generate, just mark as initialized
        console.log("No fake data mode - waiting for synced data");
        setInitialized(true);
      } else {
        // No data - generate fake data
        console.log("No existing data, generating fake data...");
        const itemCount = parseInt(params.get("items") || String(DEFAULT_ISSUE_COUNT), 10);

        generateFakeData(db, itemCount).then((userId) => {
          setCurrentUserId(userId);
          setInitialized(true);
        });
      }
    }, waitTime);
  }, [db]);

  return { initialized, currentUserId };
}
