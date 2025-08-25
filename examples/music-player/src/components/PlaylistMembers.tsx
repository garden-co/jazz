import { useState } from "react";
import { Group } from "jazz-tools";
import { Member } from "./Member";
import { MemberAccessModal } from "./MemberAccessModal";

interface PlaylistMembersProps {
  memberIds: string[];
  group: Group;
  size?: "sm" | "md" | "lg";
  className?: string;
}

export function PlaylistMembers({
  memberIds,
  group,
  size = "md",
  className = "",
}: PlaylistMembersProps) {
  const [isModalOpen, setIsModalOpen] = useState(false);

  if (!memberIds || memberIds.length === 0) return null;

  const handleMembersClick = () => {
    setIsModalOpen(true);
  };

  return (
    <>
      <button
        onClick={handleMembersClick}
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

      <MemberAccessModal
        isOpen={isModalOpen}
        onOpenChange={setIsModalOpen}
        group={group}
      />
    </>
  );
}
