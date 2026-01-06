import { useState } from "react";
import { useJazz, useAll, useOne } from "@jazz/react";
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
import type { Database } from "@/generated/client";

interface IssueDetailProps {
  issueId: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function IssueDetail({
  issueId,
  open,
  onOpenChange,
}: IssueDetailProps) {
  const db = useJazz() as unknown as Database;

  // Fetch the issue with all related data
  const [issue] = useOne(
    db.issues.with({
      project: true,
      IssueLabels: { label: true },
      IssueAssignees: { user: true },
    }),
    issueId
  );

  // Fetch reference data internally
  const [allUsers] = useAll(db.users);
  const [allLabels] = useAll(db.labels);
  const [allProjects] = useAll(db.projects);
  const [editingTitle, setEditingTitle] = useState(false);
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");

  // Extract labels and assignees from the included data
  const labels = issue?.IssueLabels.map((il) => il.label) ?? [];
  const assignees = issue?.IssueAssignees.map((ia) => ia.user) ?? [];

  const handleStatusChange = (status: string) => {
    if (!issue) return;
    db.issues.update(issue.id, { status, updatedAt: BigInt(Date.now()) });
  };

  const handlePriorityChange = (priority: string) => {
    if (!issue) return;
    db.issues.update(issue.id, { priority, updatedAt: BigInt(Date.now()) });
  };

  const handleProjectChange = (projectId: string) => {
    if (!issue) return;
    db.issues.update(issue.id, { project: projectId, updatedAt: BigInt(Date.now()) });
  };

  const handleTitleSave = () => {
    if (!issue) return;
    if (title.trim() && title !== issue.title) {
      db.issues.update(issue.id, { title: title.trim(), updatedAt: BigInt(Date.now()) });
    }
    setEditingTitle(false);
  };

  const handleDescriptionChange = (value: string) => {
    if (!issue) return;
    setDescription(value);
    db.issues.update(issue.id, { description: value, updatedAt: BigInt(Date.now()) });
  };

  const handleAddAssignee = (userId: string) => {
    if (!issue) return;
    // Check if already assigned
    if (!assignees.find((a) => a.id === userId)) {
      db.issueassignees.create({ issue: issue.id, user: userId });
    }
  };

  const handleAddLabel = (labelId: string) => {
    if (!issue) return;
    if (!labels.find((l) => l.id === labelId)) {
      db.issuelabels.create({ issue: issue.id, label: labelId });
    }
  };

  const createdDate = issue ? new Date(Number(issue.createdAt)).toLocaleDateString() : "";
  const updatedDate = issue ? new Date(Number(issue.updatedAt)).toLocaleDateString() : "";

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-[500px] sm:max-w-[500px] overflow-y-auto">
        {!issue ? (
          <div className="flex items-center justify-center h-32">
            <div className="text-muted-foreground">Loading...</div>
          </div>
        ) : (
          <>
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
            <Select value={issue.project.id} onValueChange={handleProjectChange}>
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
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}
