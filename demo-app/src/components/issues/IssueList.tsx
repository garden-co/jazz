import { useMemo } from "react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { IssueRow } from "./IssueRow";
import type {
  Issue,
  User,
  Label,
  Project,
  IssueLabel,
  IssueAssignee,
} from "@/generated/types";

interface IssueListProps {
  issues: Issue[];
  projects: Project[];
  users: User[];
  labels: Label[];
  issueLabels: IssueLabel[];
  issueAssignees: IssueAssignee[];
  onSelectIssue: (issue: Issue) => void;
  currentPage: number;
  pageSize: number;
  onNextPage: () => void;
  onPrevPage: () => void;
}

export function IssueList({
  issues,
  projects,
  users,
  labels,
  issueLabels,
  issueAssignees,
  onSelectIssue,
  currentPage,
  pageSize,
  onNextPage,
  onPrevPage,
}: IssueListProps) {
  // Build lookup maps
  const projectsById = useMemo(() => {
    const map = new Map<string, Project>();
    for (const p of projects) {
      map.set(p.id, p);
    }
    return map;
  }, [projects]);

  const usersById = useMemo(() => {
    const map = new Map<string, User>();
    for (const u of users) {
      map.set(u.id, u);
    }
    return map;
  }, [users]);

  const labelsById = useMemo(() => {
    const map = new Map<string, Label>();
    for (const l of labels) {
      map.set(l.id, l);
    }
    return map;
  }, [labels]);

  // Build labels per issue
  const labelsByIssue = useMemo(() => {
    const map = new Map<string, Label[]>();
    for (const il of issueLabels) {
      const label = labelsById.get(il.label);
      if (label) {
        const arr = map.get(il.issue) || [];
        arr.push(label);
        map.set(il.issue, arr);
      }
    }
    return map;
  }, [issueLabels, labelsById]);

  // Build assignees per issue
  const assigneesByIssue = useMemo(() => {
    const map = new Map<string, User[]>();
    for (const ia of issueAssignees) {
      const user = usersById.get(ia.user);
      if (user) {
        const arr = map.get(ia.issue) || [];
        arr.push(user);
        map.set(ia.issue, arr);
      }
    }
    return map;
  }, [issueAssignees, usersById]);

  // Pagination
  const totalPages = Math.ceil(issues.length / pageSize);
  const startIndex = currentPage * pageSize;
  const paginatedIssues = issues.slice(startIndex, startIndex + pageSize);

  if (issues.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center text-muted-foreground">
        No issues found
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <ScrollArea className="flex-1">
        {paginatedIssues.map((issue) => (
          <IssueRow
            key={issue.id}
            issue={issue}
            project={projectsById.get(issue.project)}
            assignees={assigneesByIssue.get(issue.id) || []}
            labels={labelsByIssue.get(issue.id) || []}
            onClick={() => onSelectIssue(issue)}
          />
        ))}
      </ScrollArea>

      {totalPages > 1 && (
        <div className="flex items-center justify-between border-t px-4 py-2">
          <span className="text-sm text-muted-foreground">
            Showing {startIndex + 1}-{Math.min(startIndex + pageSize, issues.length)} of {issues.length}
          </span>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={onPrevPage}
              disabled={currentPage === 0}
            >
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={onNextPage}
              disabled={currentPage >= totalPages - 1}
            >
              Next
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
