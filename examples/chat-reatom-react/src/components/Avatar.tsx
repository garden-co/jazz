import multiavatar from "@multiavatar/multiavatar/esm";

interface AvatarProps {
  profileId: string;
  avatarData?: string;
  className?: string;
  size?: number;
}

export function Avatar({ profileId, avatarData, className, size = 32 }: AvatarProps) {
  if (avatarData) {
    return (
      <img
        src={avatarData}
        alt="Avatar"
        className={`shrink-0 rounded-full object-cover ${className ?? ""}`}
        style={{ width: size, height: size }}
      />
    );
  }

  const svg = multiavatar(profileId);
  const dataUrl = `data:image/svg+xml;base64,${btoa(svg)}`;

  return (
    <img
      src={dataUrl}
      alt="Avatar"
      className={`shrink-0 rounded-full ${className ?? ""}`}
      style={{ width: size, height: size }}
    />
  );
}
