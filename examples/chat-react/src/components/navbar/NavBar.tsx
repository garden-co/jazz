import { Suspense, useState } from "react";
import { ListIcon, UserPenIcon } from "lucide-react";
import { Avatar } from "@/components/Avatar";
import { Profile } from "@/components/profile/Profile";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useMyProfile } from "@/hooks/useMyProfile";
import { navigate } from "@/hooks/useRouter";
import { inIframe, logOut } from "@/lib/utils";

export function NavBar() {
  return (
    <header className="px-3 pt-2 pb-3 flex items-center gap-2">
      <Suspense
        fallback={
          <div className="flex items-center gap-2 animate-pulse">
            <div className="w-10 h-10 bg-muted-foreground/20 rounded-full" />
            <div className="w-24 h-4 bg-muted-foreground/20 rounded" />
          </div>
        }
      >
        <NavBarContent />
      </Suspense>
    </header>
  );
}

function NavBarContent() {
  const [menuOpen, setMenuOpen] = useState(false);

  const myProfile = useMyProfile();
  const displayName = myProfile?.name ?? "Anonymous";

  if (inIframe) {
    return (
      <div className="mx-auto flex items-center gap-2">
        <Avatar profileId={myProfile?.id ?? ""} avatarData={myProfile?.avatar} size={32} />
        <h3>{displayName}</h3>
      </div>
    );
  }

  return (
    <>
      <DropdownMenu open={menuOpen} onOpenChange={setMenuOpen}>
        <DropdownMenuTrigger asChild>
          <button type="button" className="flex gap-2 items-center focus-visible:outline-0">
            <Avatar profileId={myProfile?.id ?? ""} avatarData={myProfile?.avatar} size={32} />
            <h3>{displayName}</h3>
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-40" align="start">
          <Profile onClose={() => setMenuOpen(false)}>
            <DropdownMenuItem
              onSelect={(evt) => {
                evt.preventDefault();
              }}
            >
              <UserPenIcon />
              Profile
            </DropdownMenuItem>
          </Profile>

          <DropdownMenuItem onClick={() => navigate("/#/chats")}>
            <ListIcon />
            Chat List
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      <div className="ms-auto">
        <Button variant="ghost" onClick={logOut}>
          Log out
        </Button>
      </div>
    </>
  );
}
