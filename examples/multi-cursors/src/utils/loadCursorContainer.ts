import { Account, Group, type ID } from "jazz-tools";
import { CursorContainer, CursorFeed } from "../schema";

/**
 * Creates a new group to own the cursor container.
 * @param me - The account of the current user.
 * @returns The group.
 */
function createGroup(me: Account) {
  const group = Group.create({
    owner: me,
  });
  group.addMember("everyone", "writer");
  console.log("Created group");
  console.log(`Add "VITE_GROUP_ID=${group.$jazz.id}" to your .env file`);
  return group;
}

export async function loadGroup(me: Account, groupID: ID<Group>) {
  if (groupID === undefined) {
    console.log("No group ID found in .env, creating group...");
    return createGroup(me);
  }
  const group = await Group.load(groupID, {});
  if (group === null || group === undefined) {
    console.log("Group not found, creating group...");
    return createGroup(me);
  }
  return group;
}

/**
 * Loads the cursor container for the given cursor feed ID.
 * If the cursor container does not exist, it creates a new one.
 * If the cursor container exists, it loads the existing one.
 * @param me - The account of the current user.
 * @param cursorFeedID - The ID of the cursor feed.
 * @param groupID - The ID of the group.
 */
export async function loadCursorContainer(
  me: Account,
  cursorFeedID = "cursor-feed",
  groupID: string,
): Promise<string | undefined> {
  if (!me) return;

  const group = await loadGroup(me, groupID);

  // Using the origin as part of the unique identifier
  // to have different cursors for different origins
  const cursorUID = { feed: cursorFeedID, origin: location.origin };

  const cursorContainer = await CursorContainer.loadUnique(
    cursorUID,
    group?.$jazz.id,
  );

  if (cursorContainer === null || cursorContainer === undefined) {
    console.log("Global cursors does not exist, creating...");
    const cursorContainer = await CursorContainer.upsertUnique({
      value: {
        cursorFeed: CursorFeed.create([], group),
      },
      resolve: {
        cursorFeed: true,
      },
      unique: cursorUID,
      owner: group,
    });
    if (cursorContainer === null) {
      throw new Error("Unable to create global cursors");
    }

    return cursorContainer.cursorFeed.$jazz.id;
  } else if (cursorContainer.$jazz.refs.cursorFeed) {
    return cursorContainer.$jazz.refs.cursorFeed.id;
  } else {
    throw new Error("Cursor feed not found");
  }
}
