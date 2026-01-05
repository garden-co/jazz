import { Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import type { User } from "@/generated/types";

interface HeaderProps {
  currentUser: User | null;
  onCreateIssue: () => void;
}

export function Header({ currentUser, onCreateIssue }: HeaderProps) {
  const initials = currentUser?.name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .toUpperCase() || "?";

  return (
    <header className="flex h-14 items-center justify-between border-b px-4">
      <div className="flex items-center gap-4">
        <Button onClick={onCreateIssue} size="sm">
          <Plus className="mr-2 h-4 w-4" />
          New Issue
        </Button>
      </div>

      <div className="flex items-center gap-3">
        {currentUser && (
          <>
            <span className="text-sm text-muted-foreground">
              {currentUser.name}
            </span>
            <Avatar className="h-8 w-8">
              <AvatarFallback
                style={{ backgroundColor: currentUser.avatarColor }}
                className="text-white text-xs"
              >
                {initials}
              </AvatarFallback>
            </Avatar>
          </>
        )}
      </div>
    </header>
  );
}
