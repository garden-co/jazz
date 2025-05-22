import { Group, co, z } from 'jazz-tools';

export const SharedFile = co.map({
  name: z.string(),
  file: co.fileStream(),
  createdAt: z.date(),
  uploadedAt: z.date(),
  size: z.number(),
});

export type SharedFile = typeof SharedFile;

export const FileShareAccountRoot = co.map({
  type: z.literal('file-share-account'),
  sharedFiles: co.list(SharedFile),
})

export const FileShareAccount = co.account({
  profile: co.profile(),
  root: FileShareAccountRoot,
}).withMigration((account) => {
  if (account.root === undefined) {
    const publicGroup = Group.create({ owner: account });
    publicGroup.addMember('everyone', 'reader');

    account.root = FileShareAccountRoot.create({
      type: 'file-share-account',
      sharedFiles: co.list(SharedFile).create([], publicGroup),
    }, publicGroup);
  }
});
