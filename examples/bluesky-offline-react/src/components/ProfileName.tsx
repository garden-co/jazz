export type ProfileIdentity = {
  displayName?: string | null;
  handle?: string | null;
};

export function profileNameParts(profile: ProfileIdentity | null | undefined, fallback: string) {
  const displayName = profile?.displayName?.trim();
  const rawHandle = profile?.handle?.trim().replace(/^@/, "");
  const handle = rawHandle ? `@${rawHandle}` : undefined;

  if (displayName && handle) return { name: displayName, handle };
  return { name: displayName ?? handle ?? fallback };
}

export function ProfileName({
  profile,
  fallback,
}: {
  profile?: ProfileIdentity | null;
  fallback: string;
}) {
  const { name, handle } = profileNameParts(profile, fallback);
  return (
    <span className="profile-name">
      <strong>{name}</strong>
      {handle && (
        <>
          <span className="profile-name-separator" aria-hidden="true">
            •
          </span>
          <span className="profile-handle">{handle}</span>
        </>
      )}
    </span>
  );
}
