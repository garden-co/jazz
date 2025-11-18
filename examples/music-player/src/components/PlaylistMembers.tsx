import { Member } from "./Member";

interface PlaylistMembersProps {
  memberIds: string[];
  size?: "sm" | "md" | "lg";
  onClick: () => void;
  className?: string;
}

export function PlaylistMembers({
  memberIds,
  size = "md",
  className = "",
  onClick,
}: PlaylistMembersProps) {
  if (!memberIds || memberIds.length === 0) return null;
  return (
    <>
      <button
        onClick={onClick}
        className={`flex items-center space-x-2 hover:scale-105 transition-transform duration-200 cursor-pointer ${className}`}
        title="Click to manage playlist members"
      >
        <div className="flex -space-x-2">
          {memberIds.map((memberId) => (
            <Member
              key={memberId}
              accountId={memberId}
              size={size}
              showTooltip={true}
            />
          ))}
        </div>
        <span className="text-sm text-gray-600 ml-2">
          ({memberIds.length} member{memberIds.length !== 1 ? "s" : ""})
        </span>
      </button>
    </>
  );
}
