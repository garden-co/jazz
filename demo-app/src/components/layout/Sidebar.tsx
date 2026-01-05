import { useMemo } from "react";
import { Layers, User, Users } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";
import type { Project, Issue } from "@/generated/types";

interface SidebarProps {
  projects: Project[];
  issues: Issue[];
  selectedProjectId: string | null;
  onSelectProject: (id: string | null) => void;
  showMyIssues: boolean;
  onToggleMyIssues: (show: boolean) => void;
  currentUserId: string | null;
}

export function Sidebar({
  projects,
  issues,
  selectedProjectId,
  onSelectProject,
  showMyIssues,
  onToggleMyIssues,
}: SidebarProps) {
  // Count issues per project
  const countsByProject = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const issue of issues) {
      counts[issue.project] = (counts[issue.project] || 0) + 1;
    }
    return counts;
  }, [issues]);

  const totalCount = issues.length;

  return (
    <div className="flex h-full w-64 flex-col border-r bg-muted/30">
      <div className="p-4">
        <h1 className="text-xl font-bold text-foreground">Issue Tracker</h1>
      </div>

      <Separator />

      <div className="p-2">
        <Button
          variant={!selectedProjectId && !showMyIssues ? "secondary" : "ghost"}
          className="w-full justify-start gap-2"
          onClick={() => {
            onSelectProject(null);
            onToggleMyIssues(false);
          }}
        >
          <Layers className="h-4 w-4" />
          All Issues
          <span className="ml-auto text-xs text-muted-foreground">
            {totalCount}
          </span>
        </Button>

        <Button
          variant={showMyIssues ? "secondary" : "ghost"}
          className="w-full justify-start gap-2"
          onClick={() => {
            onSelectProject(null);
            onToggleMyIssues(true);
          }}
        >
          <User className="h-4 w-4" />
          My Issues
        </Button>
      </div>

      <Separator />

      <div className="p-2">
        <div className="flex items-center gap-2 px-2 py-1.5 text-sm font-medium text-muted-foreground">
          <Users className="h-4 w-4" />
          Projects
        </div>
      </div>

      <ScrollArea className="flex-1 px-2">
        <div className="space-y-1 pb-4">
          {projects.map((project) => (
            <Button
              key={project.id}
              variant={selectedProjectId === project.id ? "secondary" : "ghost"}
              className="w-full justify-start gap-2"
              onClick={() => {
                onSelectProject(project.id);
                onToggleMyIssues(false);
              }}
            >
              <div
                className="h-3 w-3 rounded-full"
                style={{ backgroundColor: project.color }}
              />
              <span className="truncate">{project.name}</span>
              <span className="ml-auto text-xs text-muted-foreground">
                {countsByProject[project.id] || 0}
              </span>
            </Button>
          ))}
        </div>
      </ScrollArea>
    </div>
  );
}
