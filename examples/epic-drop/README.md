# EpicDrop

EpicDrop is a file browser for large binary values in Jazz. Files stream in and out of a `bytes`
column, previews read only the range they need, and folders are shared with other people through
invite links. It runs as a Vite and React single-page app on an anonymous local-first account.

## What you can do

- Create folders and subfolders, and move between them with the folder tree or the breadcrumbs.
- Upload files by dropping them on the upload area, on a folder in the tree, or on a folder row.
  Each upload shows its progress and can be cancelled; a cancelled upload leaves no file behind.
- Sort the file table by name, type, size, modified time or owner.
- Rename, move (drag a row onto a folder, or use the Move dialog) and delete files and folders.
  Deleting asks for confirmation. Editors of a shared folder rename and delete what is inside it
  and move their own uploads; folders, and other people's files, move only within their owner's
  own folders.
- Preview files. Images, audio, video and PDFs play from a Blob, so audio and video are seekable.
  Text files show their first 64 KB and load more on request. Anything else shows its first
  512 bytes as hex.
- Download any file.
- Share a folder you own: create a "Can view" or "Can edit" invite link, change a member's access,
  remove members and revoke links. Folders shared with you appear under "Shared with me", with
  their subfolders and files.

## How it uses Jazz

- **Upload.** `File.stream()` is wrapped in an async generator that counts bytes and stops when the
  upload is cancelled, and then handed to `db.insertStreaming`. The app never holds the whole file
  as one `Uint8Array` (`src/large-values.ts`).
- **Listing.** The folder query selects metadata columns and `$updatedAt`, never `contents`
  (`src/file-list-query.ts`).
- **Range reads.** Text and hex previews use a partial large-value selection,
  `select({ contents: { from, to } })`, so only that page crosses into JavaScript. Download and
  media previews select the whole value.
- **Sharing.** `folderMembers` rows grant access; `folderInvites` rows hold invite codes that only the
  folder owner can read. Joining inserts a membership that carries the code and role, and the
  permission for that insert checks for a matching invite at the sync server
  (`permissions.ts`, `src/sharing.ts`). Because a membership row keeps the code it was created
  with, only the member and the folder owner can read it. The code travels in the URL fragment, so
  it stays out of server logs.
- **Inherited access.** A folder is readable by its owner, by its members and by anyone who can read
  its parent; editing its contents follows the same shape with editor members. Inheritance uses
  `allowedTo.read("parent", { maxDepth: 8 })`, so a share reaches eight levels of subfolders, and
  the app stops offering "New folder" at that depth. Files follow their folder. Uploads are stamped
  with the uploader, and neither renames nor moves can change an owner.
- **Moving and deleting.** Every folder in a tree belongs to the owner of its top-level folder: a
  subfolder takes its parent's owner even when an editor creates it. Only a folder's owner changes
  its parent, and only to another folder they own; editors rename in place. A file changes folder
  only by its uploader, or by the tree's owner between two of their folders. Otherwise an editor
  could move a shared folder, a subfolder or a file under one of their own folders and so share it
  with whoever can see that one.
  Deleting a folder takes its owner or someone who can edit its parent, so an invite to a folder
  never lets you delete the folder itself. A folder's delete is one transaction with its subfolders,
  files and sharing rows, so the server accepts or rejects the whole tree.
- **Names.** A profile's row id is the account id, written with `upsert`, so tabs never create two.
  Names are visible across a membership (owners see members, members see the owner) through
  reverse relations, and anyone else shows a generated name.
- **What the UI offers.** Buttons, menu items, drop targets and move destinations come from
  `db.canInsert`, `db.canUpdate` and `db.canDelete` (`src/use-browser-advice.ts`), not from a copy
  of the rules. An action appears once Jazz has answered. Jazz currently answers "unknown" for any
  check whose policy uses bounded recursive `allowedTo`, even for the folder's owner
  ([#3769](https://github.com/garden-co/jazz/issues/3769)); until that is fixed, "unknown" alone
  falls back to one temporary hint in `src/use-browser-advice.ts`: the user's own tree and folders
  under one where they are an editor. The sync server decides every write either way, and a
  rejected one shows as a toast.

## Run and test

```bash
pnpm --filter epic-drop dev
pnpm --filter epic-drop test
cargo test -p jazz-example-epic-drop-benchmark
```

`pnpm test` runs two suites:

- `tests/permissions` checks the sharing rules against a local Jazz server: private by default,
  joining only with a live invite of the same folder, role and owner, read-only viewers, editors
  who cannot take over ownership, manage access, carry a shared folder, subfolder or file out to a
  folder of their own, or delete the shared folder, names visible only across a membership, and
  revocation.
- `tests/browser` covers multi-chunk upload with a metadata-only listing, a cancelled upload that
  publishes nothing before a clean retry, file and folder authority for moves, whole-value
  download, range previews (including ranges clamped at the end of a file), and a second account
  joining a shared folder through an invite link and reading a range from it.

`benchmarks/benches/walltime.rs` times uploads (4 and 64 MiB), a 100-file folder listing, a 4 MiB
download and a 64 KiB seek into a 64 MiB file on CodSpeed wall time; `benchmarks/metadata.ts`
documents each case for the examples page.

## Known limits

- Range reads may still materialise the whole value inside Jazz before slicing it
  ([#2090](https://github.com/garden-co/jazz/issues/2090),
  [#3471](https://github.com/garden-co/jazz/issues/3471)). The app's calls stay the same when that
  lands.
- Audio and video previews load the whole file into a Blob before playing. Streaming playback
  would need range reads behind an HTTP-style range source, such as a service worker.
- `size_bytes` is a 32-bit integer, so files over 2 GB are refused before upload.
- Invite links are bearer capabilities. Revoking a link stops new joins; removing a member ends
  their access. Permissions keep shared folders and files inside the owner's
  tree, but cannot stop someone who can read a file from downloading it and uploading a copy.
- Names of other members (for example a fellow editor who uploaded a file) are not visible to
  each other, only to the owner. Recursive reverse inheritance with `maxDepth`, which would allow
  "anyone who can read a folder this account owns", is not supported by the server yet ("bounded
  SELECT INHERITS under INHERITS_REFERENCING is unsupported",
  [#3210](https://github.com/garden-co/jazz/issues/3210)).
- A folder moved into its own subfolder is prevented by the app, not by a permission rule.
- Large-value relay between browsers that use the persistent worker is tracked in
  [#1978](https://github.com/garden-co/jazz/issues/1978); remote chunk withholding in
  [#1862](https://github.com/garden-co/jazz/issues/1862).

## Follow-up

A native mounted folder (a FUSE or File Provider view of the same tables, with partial residency
and cache eviction) is the next step for EpicDrop. It is not part of this web app.
