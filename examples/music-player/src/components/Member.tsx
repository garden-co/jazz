import { useCoState } from "jazz-tools/react";
import { MusicaAccount } from "@/1_schema";
import { Image } from "jazz-tools/react";

interface MemberProps {
  accountId: string;
  size?: "sm" | "md" | "lg";
  showTooltip?: boolean;
  className?: string;
}

export function Member({
  accountId,
  size = "md",
  showTooltip = true,
  className = "",
}: MemberProps) {
  const account = useCoState(MusicaAccount, accountId, {
    resolve: { profile: true },
  });

  if (!account) {
    return (
      <div
        className={`rounded-full bg-gray-200 border-2 border-white flex items-center justify-center ${getSizeClasses(size)} ${className}`}
      >
        <span className="text-gray-500 text-xs">👤</span>
      </div>
    );
  }

  const avatar = account.profile?.avatar;
  const name = account.profile?.name || "Unknown User";

  return (
    <div
      className={`rounded-full border-2 border-white overflow-hidden ${getSizeClasses(size)} ${className}`}
      title={showTooltip ? name : undefined}
    >
      {avatar ? (
        <Image
          imageId={avatar.$jazz.id}
          width={getSizePx(size)}
          height={getSizePx(size)}
          alt={`${name}'s avatar`}
          className="w-full h-full object-cover"
        />
      ) : (
        <div className="w-full h-full bg-gray-200 flex items-center justify-center">
          <span className="text-gray-500 text-sm">
            {name.charAt(0).toUpperCase()}
          </span>
        </div>
      )}
    </div>
  );
}

function getSizeClasses(size: "sm" | "md" | "lg"): string {
  switch (size) {
    case "sm":
      return "w-6 h-6";
    case "md":
      return "w-8 h-8";
    case "lg":
      return "w-10 h-10";
    default:
      return "w-8 h-8";
  }
}

function getSizePx(size: "sm" | "md" | "lg"): number {
  switch (size) {
    case "sm":
      return 24;
    case "md":
      return 32;
    case "lg":
      return 40;
    default:
      return 32;
  }
}
