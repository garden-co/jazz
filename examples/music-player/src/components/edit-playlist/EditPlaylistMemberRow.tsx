import type { Account, Group } from "jazz-tools";
import type { MemberRole } from "@/hooks/useMemberChanges";
import type { ReactNode } from "react";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Member } from "../Member";
import { Crown, Edit, Eye, Trash2, User } from "lucide-react";
import { MusicaAccount } from "@/1_schema";
import { useSuspenseAccount } from "jazz-tools/react";

interface EditPlaylistMemberRowProps {
  member: Account;
  group: Group;
  effectiveRole: string | MemberRole | undefined;
  isPendingRemoval: boolean;
  onRoleChange: (newRole: MemberRole) => void;
  onToggleRemove: () => void;
}

function getRoleIcon(role: string | undefined): ReactNode {
  switch (role) {
    case "admin":
      return <Crown className="w-4 h-4 text-yellow-600" />;
    case "manager":
      return <Crown className="w-4 h-4 text-purple-600" />;
    case "writer":
      return <Edit className="w-4 h-4 text-blue-600" />;
    case "reader":
      return <Eye className="w-4 h-4 text-green-600" />;
    default:
      return <User className="w-4 h-4 text-gray-600" />;
  }
}

function getRoleLabel(role: string | undefined) {
  switch (role) {
    case "admin":
      return "Owner";
    case "manager":
      return "Manager";
    case "writer":
      return "Writer";
    case "reader":
      return "Reader";
    default:
      return "No Access";
  }
}

function getRoleColor(role: string | undefined) {
  switch (role) {
    case "admin":
      return "bg-yellow-100 text-yellow-800 border-yellow-200";
    case "manager":
      return "bg-purple-100 text-purple-800 border-purple-200";
    case "writer":
      return "bg-blue-100 text-blue-800 border-blue-200";
    case "reader":
      return "bg-green-100 text-green-800 border-green-200";
    default:
      return "bg-gray-100 text-gray-800 border-gray-200";
  }
}

export function EditPlaylistMemberRow({
  member,
  group,
  effectiveRole,
  isPendingRemoval,
  onRoleChange,
  onToggleRemove,
}: EditPlaylistMemberRowProps) {
  const me = useSuspenseAccount(MusicaAccount);
  const isCurrentUser = member.$jazz.id === me.$jazz.id;
  const memberId = member.$jazz.id;
  const isAdmin = group.myRole() === "admin";
  const isManager = group.myRole() === "manager" || isAdmin;

  const isMemberAdmin = group.getRoleOf(member.$jazz.id) === "admin";

  const canModify = !isMemberAdmin && isManager;

  return (
    <div className="border rounded-lg p-4">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div className="flex items-center gap-3 min-w-0">
          <Member accountId={memberId} size="sm" showTooltip={true} />
          <div className="min-w-0">
            <p className="font-medium text-gray-900 truncate">
              {isCurrentUser
                ? "You"
                : member.profile.$isLoaded
                  ? member.profile.name
                  : ""}
            </p>
            <div className="flex items-center gap-2 mt-1">
              {isPendingRemoval ? (
                <Trash2 className="w-4 h-4 text-red-600" />
              ) : (
                getRoleIcon(effectiveRole)
              )}
              <Badge
                variant="outline"
                className={
                  isPendingRemoval
                    ? "bg-red-50 text-red-800 border-red-200"
                    : getRoleColor(effectiveRole)
                }
              >
                {isPendingRemoval ? "Removed" : getRoleLabel(effectiveRole)}
              </Badge>
            </div>
          </div>
        </div>

        {canModify && (
          <div className="flex flex-col sm:flex-row sm:items-center gap-2 sm:justify-end">
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                aria-label="Grant reader access"
                onClick={() => onRoleChange("reader")}
                disabled={isPendingRemoval || effectiveRole === "reader"}
                className="px-2 py-1 text-xs w-full sm:w-auto"
              >
                <Eye className="w-3 h-3 mr-1" />
                Reader
              </Button>
              <Button
                variant="outline"
                size="sm"
                aria-label="Grant writer access"
                onClick={() => onRoleChange("writer")}
                disabled={isPendingRemoval || effectiveRole === "writer"}
                className="px-2 py-1 text-xs w-full sm:w-auto"
              >
                <Edit className="w-3 h-3 mr-1" />
                Writer
              </Button>
              {isAdmin && (
                <Button
                  variant="outline"
                  size="sm"
                  aria-label="Grant manager access"
                  onClick={() => onRoleChange("manager")}
                  disabled={isPendingRemoval || effectiveRole === "manager"}
                  className="px-2 py-1 text-xs w-full sm:w-auto"
                >
                  <Crown className="w-3 h-3 mr-1" />
                  Manager
                </Button>
              )}
            </div>
            <Button
              variant="destructive"
              size="sm"
              aria-label="Remove member"
              onClick={onToggleRemove}
              className="px-2 py-1 text-xs w-full sm:w-auto"
            >
              <Trash2 className="w-3 h-3 mr-1" />
              {isPendingRemoval ? "Undo" : "Remove"}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
