import { createInviteLink } from "jazz-react";
import { Group, Loaded, co } from "jazz-tools";
import { InitFor } from "jazz-tools";
import { Folder, PasswordItem, PasswordManagerAccount } from "./1_schema";
import { PasswordItemFormValues } from "./types";

export const saveItem = (
  item: InitFor<typeof PasswordItem>,
): Loaded<typeof PasswordItem> => {
  const passwordItem = PasswordItem.create(item, {
    owner: item.folder!._owner,
  });
  passwordItem.folder?.items?.push(passwordItem);
  return passwordItem;
};

export const updateItem = (
  item: Loaded<typeof PasswordItem>,
  values: PasswordItemFormValues,
): Loaded<typeof PasswordItem> => {
  item.applyDiff(values as Partial<InitFor<typeof PasswordItem>>);
  return item;
};

export const deleteItem = (item: Loaded<typeof PasswordItem>): void => {
  const found = item.folder?.items?.findIndex(
    (passwordItem) => passwordItem?.id === item.id,
  );
  if (found !== undefined && found > -1) item.folder?.items?.splice(found, 1);
};

export const createFolder = (
  folderName: string,
  me: Loaded<typeof PasswordManagerAccount> | null | undefined,
): Loaded<typeof Folder> => {
  const group = Group.create(me ? { owner: me } : undefined);
  const folder = Folder.create(
    {
      name: folderName,
      items: co.list(PasswordItem).create([], { owner: group }),
    },
    { owner: group },
  );
  me?.root?.folders?.push(folder);
  return folder;
};

export const shareFolder = (
  folder: Loaded<typeof Folder>,
  permission: "reader" | "writer" | "admin",
): string | undefined => {
  if (folder._owner && folder.id) {
    return createInviteLink(folder, permission);
  }
  return undefined;
};

export async function addSharedFolder(
  sharedFolderId: string,
  me: Loaded<typeof PasswordManagerAccount> | null | undefined,
) {
  const [sharedFolder, account] = await Promise.all([
    Folder.load(sharedFolderId),
    me
      ? PasswordManagerAccount.load(me?.id, {
          resolve: { root: { folders: true } },
        })
      : undefined,
  ]);

  if (!sharedFolder || !account) return;

  if (!account.root?.folders) return;

  const found = account.root.folders.some((f) => f?.id === sharedFolder.id);

  if (!found) {
    account.root.folders.push(sharedFolder);
  }
}
