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
  console.log(`Add "VITE_GROUP_ID=${group.id}" to your .env file`);
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
  console.log("Loading group...");
  const group = await loadGroup(me, groupID);

  const cursorContainer = await CursorContainer.loadUnique(
    cursorFeedID,
    group?.id,
    {
      resolve: {
        cursorFeed: true,
      },
    },
  );
  console.log(`Loading cursor container: ${cursorContainer?.id}`);

  if (cursorContainer === null || cursorContainer === undefined) {
    console.log("Global cursors does not exist, creating...");
    const cursorContainer = CursorContainer.create(
      {
        cursorFeed: CursorFeed.create([], group),
      },
      {
        owner: group,
        unique: cursorFeedID,
      },
    );
    console.log("Created global cursors", cursorContainer.id);
    if (cursorContainer.cursorFeed === null) {
      throw new Error("cursorFeed is null");
    }
    return cursorContainer.cursorFeed.id;
  } else {
    console.log(
      "Global cursors already exists, loading...",
      cursorContainer.id,
    );
    return cursorContainer.cursorFeed?.id;
  }
}
