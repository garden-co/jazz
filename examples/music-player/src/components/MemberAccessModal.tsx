import { Account, Group } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import { User, Crown, Edit, Eye, Trash2, Users } from "lucide-react";
import { MusicaAccount } from "@/1_schema";
import { Member } from "./Member";

interface MemberAccessModalProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  group: Group;
}

export function MemberAccessModal(props: MemberAccessModalProps) {
  const group = useCoState(Group, props.group.$jazz.id);

  if (!group) return null;

  // Get all members from the group
  const members = group.members.map((m) => m.account);
  const currentUser = MusicaAccount.getMe();
  const isCurrentUserAdmin = group.myRole() === "admin";

  const handleRoleChange = async (
    member: Account,
    newRole: "reader" | "writer",
  ) => {
    if (!isCurrentUserAdmin) return;

    if (newRole === "reader") {
      group.addMember(member, "reader");
    } else if (newRole === "writer") {
      group.addMember(member, "writer");
    }
  };

  const handleRemoveMember = async (member: Account) => {
    if (!isCurrentUserAdmin) return;

    group.removeMember(member);
  };

  const getRoleIcon = (role: string | undefined) => {
    switch (role) {
      case "admin":
        return <Crown className="w-4 h-4 text-yellow-600" />;
      case "writer":
        return <Edit className="w-4 h-4 text-blue-600" />;
      case "reader":
        return <Eye className="w-4 h-4 text-green-600" />;
      default:
        return <User className="w-4 h-4 text-gray-600" />;
    }
  };

  const getRoleLabel = (role: string | undefined) => {
    switch (role) {
      case "admin":
        return "Admin";
      case "writer":
        return "Writer";
      case "reader":
        return "Reader";
      default:
        return "No Access";
    }
  };

  const getRoleColor = (role: string | undefined) => {
    switch (role) {
      case "admin":
        return "bg-yellow-100 text-yellow-800 border-yellow-200";
      case "writer":
        return "bg-blue-100 text-blue-800 border-blue-200";
      case "reader":
        return "bg-green-100 text-green-800 border-green-200";
      default:
        return "bg-gray-100 text-gray-800 border-gray-200";
    }
  };

  const canModifyMember = (member: Account) => {
    return isCurrentUserAdmin && member.$jazz.id !== currentUser?.$jazz.id;
  };

  return (
    <Dialog open={props.isOpen} onOpenChange={props.onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Users className="w-5 h-5" />
            Manage Playlist Members
          </DialogTitle>
          <DialogDescription>
            Manage access levels and remove members from the playlist group.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {members.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              No members found in this playlist.
            </div>
          ) : (
            members.map((member) => {
              const memberId = member.$jazz.id;
              const currentRole = group.getRoleOf(memberId);
              const isCurrentUser = memberId === currentUser?.$jazz.id;
              const canModify = canModifyMember(member);

              return (
                <div key={memberId} className="border rounded-lg p-4">
                  <div className="flex items-center justify-between">
                    {/* Member Info */}
                    <div className="flex items-center gap-3 flex-1">
                      <Member
                        accountId={memberId}
                        size="sm"
                        showTooltip={true}
                      />
                      <div className="flex-1">
                        <p className="font-medium text-gray-900">
                          {isCurrentUser ? "You" : member.profile?.name}
                        </p>
                        <div className="flex items-center gap-2 mt-1">
                          {getRoleIcon(currentRole)}
                          <Badge
                            variant="outline"
                            className={getRoleColor(currentRole)}
                          >
                            {getRoleLabel(currentRole)}
                          </Badge>
                        </div>
                      </div>
                    </div>

                    {/* Action Buttons */}
                    {canModify && (
                      <div className="flex items-center gap-2">
                        <div className="flex gap-1">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleRoleChange(member, "reader")}
                            disabled={currentRole === "reader"}
                            className="px-2 py-1 text-xs"
                          >
                            <Eye className="w-3 h-3 mr-1" />
                            Reader
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleRoleChange(member, "writer")}
                            disabled={currentRole === "writer"}
                            className="px-2 py-1 text-xs"
                          >
                            <Edit className="w-3 h-3 mr-1" />
                            Writer
                          </Button>
                        </div>
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={() => handleRemoveMember(member)}
                          className="px-2 py-1 text-xs"
                        >
                          <Trash2 className="w-3 h-3 mr-1" />
                          Remove
                        </Button>
                      </div>
                    )}
                  </div>
                </div>
              );
            })
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => props.onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
