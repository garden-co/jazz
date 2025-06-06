import { CoListSchema, Group, co, z } from "jazz-tools";

export const PasswordItem = co.map({
  name: z.string(),
  username: z.optional(z.string()),
  username_input_selector: z.optional(z.string()),
  password: z.string(),
  password_input_selector: z.optional(z.string()),
  uri: z.optional(z.string()),
  get folder() {
    return Folder;
  },
  deleted: z.boolean(),
});

export const Folder = co.map({
  name: z.string(),
  get items(): CoListSchema<typeof PasswordItem> {
    return co.list(PasswordItem);
  },
});

export const PasswordManagerAccountRoot = co.map({
  folders: co.list(Folder),
});

export const PasswordManagerAccount = co
  .account({
    profile: co.profile(),
    root: PasswordManagerAccountRoot,
  })
  .withMigration(async (account) => {
    if (!account.root) {
      const group = Group.create({ owner: account });
      const firstFolder = Folder.create(
        {
          name: "Default",
          items: co.list(PasswordItem).create([], { owner: group }),
        },
        { owner: group },
      );

      firstFolder.items?.push(
        PasswordItem.create(
          {
            name: "Gmail",
            username: "user@gmail.com",
            password: "password123",
            uri: "https://gmail.com",
            folder: firstFolder,
            deleted: false,
          },
          { owner: group },
        ),
      );

      firstFolder.items?.push(
        PasswordItem.create(
          {
            name: "Facebook",
            username: "user@facebook.com",
            password: "facebookpass",
            uri: "https://facebook.com",
            folder: firstFolder,
            deleted: false,
          },
          { owner: group },
        ),
      );

      account.root = PasswordManagerAccountRoot.create(
        {
          folders: co.list(Folder).create([firstFolder], {
            owner: account,
          }),
        },
        { owner: account },
      );
    }
  });
