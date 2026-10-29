"use client";

import { useMemo } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";
import { JazzRecordPlayerStore } from "../src/record-player";

export function useStore(): JazzRecordPlayerStore {
  const db = useDb();
  return useMemo(() => new JazzRecordPlayerStore(db), [db]);
}

/** The signed-in user's Jazz account ID: the value other people invite. */
export function useAccountId(): string | undefined {
  return useSession()?.user.account ?? undefined;
}

/**
 * First reads on a fresh device: an empty local result waits for the server's
 * answer, so the library doesn't flash its empty state before sync arrives.
 */
export const FIRST_READ = { tier: "local-first", firstLoadRemoteWaitMs: 5_000 } as const;

export type Album = { id: string; title: string; artist: string; cover_mime?: string | null };

/** Metadata-only: the shelf never selects cover or audio bytes, only whether a cover exists. */
export function useAlbums() {
  return useAll(
    app.albums.orderBy("title", "asc").limit(200).select("title", "artist", "cover_mime"),
    FIRST_READ,
  );
}

export type PlaylistSummary = {
  id: string;
  name: string;
  role: "owner" | "editor" | "listener";
  canEdit: boolean;
};

/** Every playlist the read policy lets this account see, with its role on each. */
export function usePlaylists(): { playlists: PlaylistSummary[]; isLoading: boolean } {
  const me = useAccountId();
  const playlists = useAll(app.playlists.select("name", "$createdBy"), FIRST_READ);
  const accepted = useAll(
    me
      ? app.invitations.where({ subject: me, status: "accepted" }).select("playlist_id", "role")
      : undefined,
  );
  return useMemo(() => {
    const roles = new Map<string, "editor" | "listener">();
    for (const invite of accepted.data ?? []) {
      if (invite.role === "editor" || !roles.has(invite.playlist_id)) {
        roles.set(invite.playlist_id, invite.role);
      }
    }
    const rows = (playlists.data ?? []).map((playlist): PlaylistSummary => {
      const role =
        playlist.$createdBy?.account === me ? "owner" : (roles.get(playlist.id) ?? "listener");
      return { id: playlist.id, name: playlist.name, role, canEdit: role !== "listener" };
    });
    rows.sort((a, b) => a.name.localeCompare(b.name));
    return { playlists: rows, isLoading: playlists.data === undefined };
  }, [playlists.data, accepted.data, me]);
}
