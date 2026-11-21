import { createContext, useContext, useMemo, type ReactNode } from "react";
import { useQuery } from "convex/react";
import { Avatar, type AvatarSize } from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Profile } from "./model";
import { useObjectUrl } from "./use-object-url";

export interface ProfileDirectory {
  me: Profile;
  byId: Map<string, Profile>;
  byAuthor: Map<string, Profile>;
}

const EMPTY: Profile[] = [];
const DirectoryContext = createContext<ProfileDirectory | null>(null);

/**
 * `profiles.directory` returns this account's own profile, its co-members'
 * and those of people asking to join its rooms: the people it knows. One subscription feeds every name/avatar.
 */
export function ProfileDirectoryProvider({ me, children }: { me: Profile; children: ReactNode }) {
  const profiles = useQuery(api.profiles.directory) ?? EMPTY;
  const directory = useMemo<ProfileDirectory>(() => {
    const byId = new Map<string, Profile>();
    const byAuthor = new Map<string, Profile>();
    for (const profile of [...profiles, me]) {
      byId.set(profile._id, profile);
      if (!byAuthor.has(profile.author) || profile._id === me._id)
        byAuthor.set(profile.author, profile);
    }
    return { me, byId, byAuthor };
  }, [profiles, me]);
  return <DirectoryContext.Provider value={directory}>{children}</DirectoryContext.Provider>;
}

export function useDirectory(): ProfileDirectory {
  const directory = useContext(DirectoryContext);
  if (!directory) throw new Error("useDirectory needs a ProfileDirectoryProvider");
  return directory;
}

export function displayNameFor(
  directory: ProfileDirectory,
  { profileId, author }: { profileId?: string | null; author?: string | null },
): string {
  if (author && author === directory.me.author) return "You";
  const profile =
    (profileId ? directory.byId.get(profileId) : undefined) ??
    (author ? directory.byAuthor.get(author) : undefined);
  return profile?.displayName ?? "Bandmate";
}

export function ProfileAvatar({
  profile,
  name,
  size = "md",
}: {
  profile: Profile | undefined;
  name?: string;
  size?: AvatarSize;
}) {
  const src = useObjectUrl(profile?.avatar, profile?.avatarType);
  const label = name ?? profile?.displayName ?? "Bandmate";
  return <Avatar name={label} src={src} size={size} tooltip={false} />;
}
