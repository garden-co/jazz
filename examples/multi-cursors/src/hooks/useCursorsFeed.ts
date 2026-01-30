import { useCoState } from "jazz-tools/react-core";
import { useState, useEffect } from "react";
import { CursorFeed } from "../schema";
import { co, Group } from "jazz-tools";
import { useCurrentDate } from "./useCurrentDate";

function createGroup() {
  const group = Group.create();
  group.addMember("everyone", "writer");
  console.log("Created group");
  console.log(`Add "VITE_GROUP_ID=${group.$jazz.id}" to your .env file`);
  return group;
}

const GROUP_ID = import.meta.env.VITE_GROUP_ID;

async function loadGroup() {
  if (GROUP_ID === undefined) {
    console.log("No group ID found in .env, creating group...");
    return createGroup();
  }
  const group = await co.group().load(GROUP_ID);
  if (!group.$isLoaded) {
    throw new Error("Unable to load group with ID: " + GROUP_ID);
  }
  return group;
}

/**
 * Loads or creates a cursor feed for the current day.
 *
 * The feed is uniquely identified by the cursor group, origin, and current date.
 * This ensures each day gets a fresh cursor feed, preventing accumulation of old
 * cursor data over time.
 */
export async function loadCursorFeed(date: string): Promise<CursorFeed> {
  const group = await loadGroup();

  const feed = await CursorFeed.getOrCreateUnique({
    value: [],
    unique: {
      type: "cursor-feed",
      origin: location.origin, // To isolate cursors for different origins
      date, // To isolate cursors for different days, so history is reset every day
    },
    owner: group,
  });

  if (!feed.$isLoaded) {
    throw new Error("Unable to load cursor feed");
  }

  return feed;
}

export function useCursorsFeed() {
  const [cursorFeedID, setCursorFeedID] = useState<string | undefined>(
    undefined,
  );
  const [error, setError] = useState<string | null>(null);

  // We reset the cursor feed when the date changes, so every day starts with a fresh cursor feed
  const cursors = useCoState(CursorFeed, cursorFeedID);
  const date = useCurrentDate();

  useEffect(() => {
    loadCursorFeed(date)
      .then((feed) => {
        setCursorFeedID(feed.$jazz.id);
      })
      .catch((error) => {
        setError(error.message);
      });
  }, [date]);

  return {
    cursors,
    error,
  };
}
