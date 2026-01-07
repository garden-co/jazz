import { Account, Group, deleteCoValues } from "jazz-tools";
import { MusicaAccountRoot, Playlist, MusicaAccount } from "./musicPlayerSchema";
import { Note, Document } from "./schema";
export {};

const noteId = "" as string;
const documentId = "" as string;

// #region Basic
// Delete the note (requires admin permissions)
await deleteCoValues(Note, noteId);

// After deletion, loading returns a deleted state
const deletedNote = await Note.load(noteId, { skipRetry: true });
deletedNote.$isLoaded; // false
deletedNote.$jazz.loadingState; // "deleted"
// #endregion

// #region WithResolve
// Delete the document along with all attachments and their files
await deleteCoValues(Document, documentId, {
  resolve: {
    attachments: {
      $each: {
        file: true,
      },
    },
  },
});
// #endregion

{
// #region CollectionWithInaccessible
const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
        root: {
            playlists: {
                $each: { $onError: "catch" },
            },
        },
    },
});

// Delete all playlists referenced in the user's root. 
// This may include shared playlists.
for (const playlist of me.root.playlists.values()) {
    // Skip playlists we can't even read
    if (!playlist.$isLoaded) continue;

    if (me.canAdmin(playlist)) {
        // Delete all the playlists we own
        await deleteCoValues(Playlist, playlist.$jazz.id);
    } else {
        // Remove ourselves from playlists other users shared with us
        playlist.$jazz.owner.removeMember(me);
    }
}

// #endregion
}

{
// #region Limitations
const me = MusicaAccount.getMe();
const group = Group.create();

// These calls have no effect - Groups and Accounts are silently skipped
await deleteCoValues(Group, group.$jazz.id);
await deleteCoValues(MusicaAccount, me.$jazz.id);

// This deletes the account content, but not the account itself
await deleteCoValues(MusicaAccount, me.$jazz.id, {
    resolve: {
      profile: {
        avatar: {
          $each: true,
        },
      },
      root: {
        rootPlaylist: {
          tracks: {
            $each: {
              file: true,
              waveform: true,
            },
          },
        },
        // The list content has been deleted previously
        playlists: true,
      },
    },
});
// #endregion
}

