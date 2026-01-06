import { useEffect, useState } from "react";
import { Plus, Sun, Moon } from "lucide-react";
import { useOne } from "@jazz/react";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { app } from "@/generated/client";

interface HeaderProps {
  currentUserId: string | null;
  onCreateIssue: () => void;
}

export function Header({ currentUserId, onCreateIssue }: HeaderProps) {
  // Fetch current user internally
  const [currentUser] = useOne(app.users, currentUserId);

  const [isDark, setIsDark] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("theme") === "dark" ||
        (!localStorage.getItem("theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    }
    return false;
  });

  useEffect(() => {
    if (isDark) {
      document.documentElement.classList.add("dark");
      localStorage.setItem("theme", "dark");
    } else {
      document.documentElement.classList.remove("dark");
      localStorage.setItem("theme", "light");
    }
  }, [isDark]);

  const initials = currentUser?.name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .toUpperCase() || "?";

  return (
    <header className="flex items-center justify-between border-b px-4 py-3">
      <div className="flex items-center gap-4">
        <Button onClick={onCreateIssue} size="sm">
          <Plus className="mr-2 h-4 w-4" />
          New Issue
        </Button>
      </div>

      <div className="flex items-center gap-3">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => setIsDark(!isDark)}
          className="h-8 w-8"
        >
          {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
        </Button>
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
