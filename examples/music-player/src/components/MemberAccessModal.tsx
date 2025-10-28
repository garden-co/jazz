import { useState } from "react";
import { useToast } from "@/hooks/use-toast";
import { Account, Group } from "jazz-tools";
import { useCoState, createInviteLink } from "jazz-tools/react";
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
import {
  User,
  Crown,
  Edit,
  Eye,
  Trash2,
  Users,
  UserPlus,
  Link,
} from "lucide-react";
import { MusicaAccount, Playlist } from "@/1_schema";
import { Member } from "./Member";

interface MemberAccessModalProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  playlist: Playlist;
}

export function MemberAccessModal(props: MemberAccessModalProps) {
  const group = useCoState(Group, props.playlist.$jazz.owner.$jazz.id);
  const [selectedRole, setSelectedRole] = useState<
    "reader" | "writer" | "manager"
  >("reader");
  const { toast } = useToast();

  if (!group) return null;

  // Get all members from the group
  const members = group.members.map((m) => m.account);
  const currentUser = MusicaAccount.getMe();
  const isManager = group.myRole() === "admin" || group.myRole() === "manager";

  const handleRoleChange = async (
    member: Account,
    newRole: "reader" | "writer" | "manager",
  ) => {
    if (!isManager) return;

    group.addMember(member, newRole);
  };

  const handleRemoveMember = async (member: Account) => {
    if (!isManager) return;

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
      case "manager":
        return "Manager";
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
      case "manager":
        return "bg-purple-100 text-purple-800 border-purple-200";
      case "writer":
        return "bg-blue-100 text-blue-800 border-blue-200";
      case "reader":
        return "bg-green-100 text-green-800 border-green-200";
      default:
        return "bg-gray-100 text-gray-800 border-gray-200";
    }
  };

  const canModifyMember = (member: Account) => {
    return (
      isManager &&
      (member.$jazz.id === currentUser?.$jazz.id ||
        !member.canAdmin(props.playlist))
    );
  };

  const handleGetInviteLink = async () => {
    if (!isManager) return;

    const inviteLink = createInviteLink(props.playlist, selectedRole);
    await navigator.clipboard.writeText(inviteLink);

    toast({
      title: "Invite link copied",
      description: `Invite link for ${selectedRole} role copied to clipboard.`,
    });
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
            <section>
              {members.map((member) => {
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
                              aria-label="Grant reader access"
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
                              aria-label="Grant writer access"
                              onClick={() => handleRoleChange(member, "writer")}
                              disabled={currentRole === "writer"}
                              className="px-2 py-1 text-xs"
                            >
                              <Edit className="w-3 h-3 mr-1" />
                              Writer
                            </Button>
                            {group.myRole() === "admin" && (
                              <Button
                                variant="outline"
                                size="sm"
                                aria-label="Grant manager access"
                                onClick={() =>
                                  handleRoleChange(member, "manager")
                                }
                                disabled={currentRole === "manager"}
                                className="px-2 py-1 text-xs"
                              >
                                <Edit className="w-3 h-3 mr-1" />
                                Manager
                              </Button>
                            )}
                          </div>
                          <Button
                            variant="destructive"
                            size="sm"
                            aria-label="Remove member"
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
              })}
            </section>
          )}

          {isManager && (
            <section className="border-2 border-dashed border-gray-300 rounded-lg p-6 mt-4">
              <div className="flex items-start gap-4">
                <div className="p-3 bg-blue-50 rounded-full">
                  <UserPlus className="w-6 h-6 text-blue-600" />
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold text-gray-900 mb-1">
                    Invite new members
                  </h3>
                  <p className="text-sm text-gray-600 mb-4">
                    Generate an invite link to add new members to this playlist.
                  </p>
                  <div className="flex items-center gap-3">
                    <div className="flex gap-2">
                      <Button
                        variant={
                          selectedRole === "reader" ? "default" : "outline"
                        }
                        size="sm"
                        onClick={() => setSelectedRole("reader")}
                        className="px-3 py-2"
                      >
                        <Eye className="w-4 h-4 mr-1" />
                        Reader
                      </Button>
                      <Button
                        variant={
                          selectedRole === "writer" ? "default" : "outline"
                        }
                        size="sm"
                        onClick={() => setSelectedRole("writer")}
                        className="px-3 py-2"
                      >
                        <Edit className="w-4 h-4 mr-1" />
                        Writer
                      </Button>
                      {group.myRole() === "admin" && (
                        <Button
                          variant={
                            selectedRole === "manager" ? "default" : "outline"
                          }
                          size="sm"
                          onClick={() => setSelectedRole("manager")}
                          className="px-3 py-2"
                        >
                          <Crown className="w-4 h-4 mr-1" />
                          Manager
                        </Button>
                      )}
                    </div>
                    <Button onClick={handleGetInviteLink} className="gap-2">
                      <Link className="w-4 h-4" />
                      Get Invite Link
                    </Button>
                  </div>
                </div>
              </div>
            </section>
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
