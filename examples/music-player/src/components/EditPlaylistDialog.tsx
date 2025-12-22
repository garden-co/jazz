import { useState } from "react";
import { useToast } from "@/hooks/use-toast";
import { Account, Group } from "jazz-tools";
import { createInviteLink, useSuspenseCoState } from "jazz-tools/react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Crown, Edit, Eye, Link, UserPlus, Users } from "lucide-react";
import { Playlist } from "@/1_schema";
import { updatePlaylistTitle } from "@/4_actions";
import { useMemberChanges } from "@/hooks/useMemberChanges";
import { EditPlaylistMemberRow } from "./edit-playlist/EditPlaylistMemberRow";

type EditPlaylistDialogSection = "details" | "members";
type MemberRole = "reader" | "writer" | "manager";

interface EditPlaylistDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  playlistId: string;
  defaultSection?: EditPlaylistDialogSection;
}

export function EditPlaylistDialog(props: EditPlaylistDialogProps) {
  const playlist = useSuspenseCoState(Playlist, props.playlistId);
  const group = useSuspenseCoState(Group, playlist.$jazz.owner.$jazz.id);

  const { toast } = useToast();

  const [activeSection, setActiveSection] = useState<EditPlaylistDialogSection>(
    props.defaultSection ?? "members",
  );
  const [selectedRole, setSelectedRole] = useState<
    "reader" | "writer" | "manager"
  >("reader");
  const [localTitle, setLocalTitle] = useState(playlist.title);
  const memberChanges = useMemberChanges();

  const members = group.members.map((m) => m.account);
  const isManager = group.myRole() === "admin" || group.myRole() === "manager";

  const handleRoleChange = (
    member: Account,
    currentRole: string | undefined,
    newRole: MemberRole,
  ) => {
    if (!isManager) return;
    const memberId = member.$jazz.id;
    memberChanges.stageRoleChange({ memberId, currentRole, newRole });
  };

  const handleRemoveMemberToggle = (member: Account) => {
    if (!isManager) return;
    const memberId = member.$jazz.id;
    memberChanges.toggleRemove(memberId);
  };

  const handleGetInviteLink = async () => {
    if (!isManager) return;
    const inviteLink = createInviteLink(playlist, selectedRole);
    await navigator.clipboard.writeText(inviteLink);

    toast({
      title: "Invite link copied",
      description: `Invite link for ${selectedRole} role copied to clipboard.`,
    });
  };

  const handleSaveTitle = () => {
    const nextTitle = localTitle.trim();
    if (!nextTitle) return;
    updatePlaylistTitle(playlist, nextTitle);
    props.onOpenChange(false);
  };

  const handleDiscardMemberChanges = () => {
    memberChanges.discard();
  };

  const handleApplyMemberChanges = async () => {
    if (!isManager || !memberChanges.hasPendingChanges) return;
    await memberChanges.apply({ group, members });
    toast({ title: "Member changes applied" });
  };

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      memberChanges.reset();
    }
    props.onOpenChange(open);
  };

  return (
    <Dialog open={props.isOpen} onOpenChange={handleOpenChange}>
      <DialogContent className="w-[calc(100vw-1rem)] sm:max-w-2xl h-[calc(100vh-2rem)] sm:h-auto max-h-[calc(100vh-2rem)] sm:max-h-[80vh] flex flex-col overflow-hidden">
        <DialogHeader className="px-6 pt-6">
          <DialogTitle className="flex items-center gap-2">
            <Users className="w-5 h-5" />
            Edit playlist
          </DialogTitle>
          <DialogDescription>
            Update playlist details and manage member access.
          </DialogDescription>
        </DialogHeader>

        <div className="px-6 pt-4">
          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              size="sm"
              variant={activeSection === "details" ? "default" : "outline"}
              onClick={() => setActiveSection("details")}
            >
              Details
            </Button>
            <Button
              type="button"
              size="sm"
              variant={activeSection === "members" ? "default" : "outline"}
              onClick={() => setActiveSection("members")}
            >
              Members
            </Button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto min-h-0 px-6 pb-6 pt-4">
          {activeSection === "details" ? (
            <section className="space-y-4">
              <div>
                <Label htmlFor="playlist-title" className="text-sm font-medium">
                  Playlist title
                </Label>
                <Input
                  id="playlist-title"
                  value={localTitle}
                  onChange={(e) => setLocalTitle(e.target.value)}
                  placeholder="Enter playlist title"
                  className="mt-1"
                  autoFocus
                  onKeyDown={(evt) => {
                    if (evt.key === "Enter") handleSaveTitle();
                    if (evt.key === "Escape") props.onOpenChange(false);
                  }}
                />
              </div>
            </section>
          ) : (
            <div className="space-y-4">
              {members.length === 0 ? (
                <div className="text-center py-8 text-gray-500">
                  No members found in this playlist.
                </div>
              ) : (
                <section className="space-y-3">
                  {members.map((member) => {
                    const memberId = member.$jazz.id;
                    const currentRole = group.getRoleOf(memberId);
                    const pendingChange =
                      memberChanges.pendingByMemberId[memberId];
                    const isPendingRemoval = pendingChange?.type === "remove";
                    const effectiveRole =
                      pendingChange?.type === "setRole"
                        ? pendingChange.role
                        : currentRole;

                    return (
                      <EditPlaylistMemberRow
                        key={memberId}
                        member={member}
                        group={group}
                        effectiveRole={effectiveRole}
                        isPendingRemoval={isPendingRemoval}
                        onRoleChange={(newRole) =>
                          handleRoleChange(member, currentRole, newRole)
                        }
                        onToggleRemove={() => handleRemoveMemberToggle(member)}
                      />
                    );
                  })}
                </section>
              )}

              {isManager && (
                <section className="border-2 border-dashed border-gray-300 rounded-lg p-4 sm:p-6 mt-4">
                  <div className="flex flex-col sm:flex-row sm:items-start gap-4">
                    <div className="p-3 bg-blue-50 rounded-full w-fit">
                      <UserPlus className="w-6 h-6 text-blue-600" />
                    </div>
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900 mb-1">
                        Invite new members
                      </h3>
                      <p className="text-sm text-gray-600 mb-4">
                        Generate an invite link to add new members to this
                        playlist.
                      </p>
                      <div className="flex flex-col sm:flex-row sm:items-center gap-3">
                        <div
                          className={`grid gap-2 w-full sm:w-auto ${
                            group.myRole() === "admin"
                              ? "grid-cols-3"
                              : "grid-cols-2"
                          }`}
                        >
                          <Button
                            variant={
                              selectedRole === "reader" ? "default" : "outline"
                            }
                            size="sm"
                            onClick={() => setSelectedRole("reader")}
                            className="w-full justify-center"
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
                            className="w-full justify-center"
                          >
                            <Edit className="w-4 h-4 mr-1" />
                            Writer
                          </Button>
                          {group.myRole() === "admin" && (
                            <Button
                              variant={
                                selectedRole === "manager"
                                  ? "default"
                                  : "outline"
                              }
                              size="sm"
                              onClick={() => setSelectedRole("manager")}
                              className="w-full justify-center"
                            >
                              <Crown className="w-4 h-4 mr-1" />
                              Manager
                            </Button>
                          )}
                        </div>
                        <Button
                          onClick={handleGetInviteLink}
                          className="gap-2 w-full sm:w-auto"
                        >
                          <Link className="w-4 h-4" />
                          Get Invite Link
                        </Button>
                      </div>
                    </div>
                  </div>
                </section>
              )}
            </div>
          )}
        </div>

        <DialogFooter className="px-6 py-4 border-t flex flex-col sm:flex-row gap-2">
          {activeSection === "details" ? (
            <>
              <Button
                type="button"
                variant="outline"
                onClick={() => props.onOpenChange(false)}
                className="w-full sm:w-auto"
              >
                Cancel
              </Button>
              <Button
                type="button"
                onClick={handleSaveTitle}
                disabled={!localTitle.trim()}
                className="w-full sm:w-auto"
              >
                Save
              </Button>
            </>
          ) : (
            <>
              {isManager && memberChanges.hasPendingChanges ? (
                <>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={handleDiscardMemberChanges}
                    className="w-full sm:w-auto"
                  >
                    Discard
                  </Button>
                  <Button
                    type="button"
                    onClick={handleApplyMemberChanges}
                    className="w-full sm:w-auto"
                  >
                    Apply changes
                  </Button>
                </>
              ) : (
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => props.onOpenChange(false)}
                  className="w-full sm:w-auto"
                >
                  Close
                </Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
