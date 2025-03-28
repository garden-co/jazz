import { Account, Group, type ID } from "jazz-tools";
import { CursorContainer, CursorFeed } from "../schema";

export async function loadGroup(me: Account, groupID: ID<Group>) {
  const group = await Group.load(groupID, {});
  if (group === null) {
    const group = Group.create({
      owner: me,
    });
    group.addMember("everyone", "writer");
    console.log("Created group:", group.id);
    return group;
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
  cursorFeedID: ID<CursorFeed>,
  groupID: ID<Group>,
): Promise<ID<CursorFeed> | undefined> {
  if (!me) return;
  const group = await loadGroup(me, groupID);

  const cursorContainerID = CursorContainer.findUnique(
    cursorFeedID,
    group?.id as ID<Group>,
  );
  const cursorContainer = await CursorContainer.load(cursorContainerID, {
    resolve: {
      cursorFeed: true,
    },
  });

  if (cursorContainer === null) {
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
    return cursorContainer.cursorFeed.id;
  }
}
