import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";

export class PasswordItem extends CoMap {
  name = coField.string;
  username = coField.optional.string;
  username_input_selector = coField.optional.string;
  password = coField.string;
  password_input_selector = coField.optional.string;
  uri = coField.optional.string;
  folder = coField.ref(Folder);
  deleted = coField.boolean;
}

export class PasswordList extends CoList.Of(coField.ref(PasswordItem)) {}

export class Folder extends CoMap {
  name = coField.string;
  items = coField.ref(PasswordList);
}

export class FolderList extends CoList.Of(coField.ref(Folder)) {}

export class PasswordManagerAccountRoot extends CoMap {
  folders = coField.ref(FolderList);
}

export class PasswordManagerAccount extends Account {
  profile = coField.ref(Profile);
  root = coField.ref(PasswordManagerAccountRoot);

  migrate() {
    if (!this._refs.root) {
      const group = Group.create({ owner: this });
      const firstFolder = Folder.create(
        {
          name: "Default",
          items: PasswordList.create([], { owner: group }),
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

      this.root = PasswordManagerAccountRoot.create(
        {
          folders: FolderList.create([firstFolder], {
            owner: this,
          }),
        },
        { owner: this },
      );
    }
  }
}
