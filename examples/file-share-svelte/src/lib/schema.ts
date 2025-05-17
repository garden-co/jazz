import { Account, CoList, CoMap, FileStream, Profile, coField, Group } from 'jazz-tools';

export class SharedFile extends CoMap {
  name = coField.string;
  file = coField.ref(FileStream);
  createdAt = coField.Date;
  uploadedAt = coField.Date;
  size = coField.number;
}

export class FileShareProfile extends Profile {
  name = coField.string;
}

export class ListOfSharedFiles extends CoList.Of(coField.ref(SharedFile)) {}

export class FileShareAccountRoot extends CoMap {
  type = coField.literal('file-share-account');
  sharedFiles = coField.ref(ListOfSharedFiles);
}

export class FileShareAccount extends Account {
  profile = coField.ref(FileShareProfile);
  root = coField.ref(FileShareAccountRoot);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  async migrate() {
    await this._refs.root?.load();

    // Initialize root if it doesn't exist
    if (this.root === undefined || this.root?.type !== 'file-share-account') {
      // Create a group that will own all shared files
      const publicGroup = Group.create({ owner: this });
      publicGroup.addMember('everyone', 'reader');

      this.root = FileShareAccountRoot.create(
        {
          type: 'file-share-account',
          sharedFiles: ListOfSharedFiles.create([], { owner: publicGroup }),
        },
      );
    }
  }
}
