import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { IssueRow } from "./IssueRow";
import type { LoadedIssue } from "@/App";

interface IssueListProps {
  issues: LoadedIssue[];
  onSelectIssue: (issue: LoadedIssue) => void;
  currentPage: number;
  pageSize: number;
  onNextPage: () => void;
  onPrevPage: () => void;
}

export function IssueList({
  issues,
  onSelectIssue,
  currentPage,
  pageSize,
  onNextPage,
  onPrevPage,
}: IssueListProps) {
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
