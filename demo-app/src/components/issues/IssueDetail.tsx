import { useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Separator } from "@/components/ui/separator";
import { LabelBadge } from "./LabelBadge";
import { STATUSES, STATUS_LABELS, PRIORITIES, PRIORITY_LABELS } from "@/utils/constants";
import type { Issue, User, Label, Project } from "@/generated/types";
import type { Database } from "@/generated/client";

interface IssueDetailProps {
  issue: Issue | null;
  project?: Project;
  assignees: User[];
  labels: Label[];
  allUsers: User[];
  allLabels: Label[];
  allProjects: Project[];
  db: Database;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function IssueDetail({
  issue,
  project,
  assignees,
  labels,
  allUsers,
  allLabels,
  allProjects,
  db,
  open,
  onOpenChange,
}: IssueDetailProps) {
  const [editingTitle, setEditingTitle] = useState(false);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");

  if (!issue) return null;

  const handleStatusChange = (status: string) => {
    db.issues.update(issue.id, { status, updatedAt: BigInt(Date.now()) });
  };

  const handlePriorityChange = (priority: string) => {
    db.issues.update(issue.id, { priority, updatedAt: BigInt(Date.now()) });
  };

  const handleProjectChange = (projectId: string) => {
    db.issues.update(issue.id, { project: projectId, updatedAt: BigInt(Date.now()) });
  };

  const handleTitleSave = () => {
    if (title.trim() && title !== issue.title) {
      db.issues.update(issue.id, { title: title.trim(), updatedAt: BigInt(Date.now()) });
    }
    setEditingTitle(false);
  };

  const handleDescriptionChange = (value: string) => {
    setDescription(value);
    db.issues.update(issue.id, { description: value, updatedAt: BigInt(Date.now()) });
  };

  const handleAddAssignee = (userId: string) => {
    // Check if already assigned
    if (!assignees.find((a) => a.id === userId)) {
      db.issueassignees.create({ issue: issue.id, user: userId });
    }
  };

  const handleRemoveAssignee = (userId: string) => {
    // Find the IssueAssignee record - for now we'll use a workaround
    // In a real app, we'd have the issueAssignee ID
    // For simplicity, we'll just not implement removal here
  };

  const handleAddLabel = (labelId: string) => {
    if (!labels.find((l) => l.id === labelId)) {
      db.issuelabels.create({ issue: issue.id, label: labelId });
    }
  };

  const createdDate = new Date(Number(issue.createdAt)).toLocaleDateString();
  const updatedDate = new Date(Number(issue.updatedAt)).toLocaleDateString();

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-[500px] sm:max-w-[500px] overflow-y-auto">
        <SheetHeader>
          <SheetTitle className="text-left">
            {editingTitle ? (
              <Input
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onBlur={handleTitleSave}
                onKeyDown={(e) => e.key === "Enter" && handleTitleSave()}
                autoFocus
              />
            ) : (
              <span
                className="cursor-pointer hover:text-primary"
                onClick={() => {
                  setTitle(issue.title);
                  setEditingTitle(true);
                }}
              >
                {issue.title}
              </span>
            )}
          </SheetTitle>
        </SheetHeader>

        <div className="mt-6 space-y-6">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium text-muted-foreground">
                Status
              </label>
              <Select value={issue.status} onValueChange={handleStatusChange}>
                <SelectTrigger className="mt-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUSES.map((s) => (
                    <SelectItem key={s} value={s}>
                      {STATUS_LABELS[s]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="text-sm font-medium text-muted-foreground">
                Priority
              </label>
              <Select value={issue.priority} onValueChange={handlePriorityChange}>
                <SelectTrigger className="mt-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PRIORITIES.map((p) => (
                    <SelectItem key={p} value={p}>
                      {PRIORITY_LABELS[p]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div>
            <label className="text-sm font-medium text-muted-foreground">
              Project
            </label>
            <Select value={issue.project} onValueChange={handleProjectChange}>
              <SelectTrigger className="mt-1">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {allProjects.map((p) => (
                  <SelectItem key={p.id} value={p.id}>
                    <div className="flex items-center gap-2">
                      <div
                        className="h-2 w-2 rounded-full"
                        style={{ backgroundColor: p.color }}
                      />
                      {p.name}
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Separator />

          <div>
            <label className="text-sm font-medium text-muted-foreground">
              Description
            </label>
            <Textarea
              className="mt-1"
              value={issue.description || ""}
              onChange={(e) => handleDescriptionChange(e.target.value)}
              placeholder="Add a description..."
              rows={4}
            />
          </div>

          <Separator />

          <div>
            <label className="text-sm font-medium text-muted-foreground">
              Assignees
            </label>
            <div className="mt-2 flex flex-wrap gap-2">
              {assignees.map((user) => {
                const initials = user.name
                  .split(" ")
                  .map((n) => n[0])
                  .join("")
                  .toUpperCase();
                return (
                  <div
                    key={user.id}
                    className="flex items-center gap-2 rounded-full bg-muted px-2 py-1"
                  >
                    <Avatar className="h-5 w-5">
                      <AvatarFallback
                        style={{ backgroundColor: user.avatarColor }}
                        className="text-white text-[8px]"
                      >
                        {initials}
                      </AvatarFallback>
                    </Avatar>
                    <span className="text-sm">{user.name}</span>
                  </div>
                );
              })}
            </div>
            <Select onValueChange={handleAddAssignee}>
              <SelectTrigger className="mt-2 w-[200px]">
                <SelectValue placeholder="Add assignee..." />
              </SelectTrigger>
              <SelectContent>
                {allUsers
                  .filter((u) => !assignees.find((a) => a.id === u.id))
                  .map((u) => (
                    <SelectItem key={u.id} value={u.id}>
                      {u.name}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>

          <div>
            <label className="text-sm font-medium text-muted-foreground">
              Labels
            </label>
            <div className="mt-2 flex flex-wrap gap-2">
              {labels.map((label) => (
                <LabelBadge key={label.id} name={label.name} color={label.color} />
              ))}
            </div>
            <Select onValueChange={handleAddLabel}>
              <SelectTrigger className="mt-2 w-[200px]">
                <SelectValue placeholder="Add label..." />
              </SelectTrigger>
              <SelectContent>
                {allLabels
                  .filter((l) => !labels.find((x) => x.id === l.id))
                  .map((l) => (
                    <SelectItem key={l.id} value={l.id}>
                      <div className="flex items-center gap-2">
                        <div
                          className="h-2 w-2 rounded-full"
                          style={{ backgroundColor: l.color }}
                        />
                        {l.name}
                      </div>
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>

          <Separator />

          <div className="text-xs text-muted-foreground space-y-1">
            <div>Created: {createdDate}</div>
            <div>Updated: {updatedDate}</div>
          </div>

          <Button
            variant="destructive"
            size="sm"
            onClick={() => {
              db.issues.delete(issue.id);
              onOpenChange(false);
            }}
          >
            Delete Issue
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
