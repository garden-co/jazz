import { Suspense, useState } from "react";
import {
  useIsAuthenticated,
  useLogOut,
  useSuspenseAccount,
} from "jazz-tools/react";
import { ListIcon, MenuIcon, UserPenIcon } from "lucide-react";
import { Avatar } from "@/components/Avatar";
import { AuthModal } from "@/components/navbar/AuthModal";
import { Profile } from "@/components/profile/Profile";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { navigate } from "@/hooks/useRouter";
import { inIframe } from "@/lib/utils";
import { ChatAccountWithProfile } from "@/schema";

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
  const me = useSuspenseAccount(ChatAccountWithProfile);
  const isAuthenticated = useIsAuthenticated();
  const logOut = useLogOut();
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <>
      {/* Needed for Jazz Tools homepage only */}
      {inIframe ? (
        <div className="mx-auto flex items-center gap-2">
          <Avatar profileId={me.profile.$jazz.id} />
          <h3>{me.profile.name}</h3>
        </div>
      ) : (
        <>
          <DropdownMenu open={menuOpen} onOpenChange={setMenuOpen}>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="flex gap-2 items-center focus-visible:outline-0"
              >
                <MenuIcon />
                <Avatar profileId={me.profile.$jazz.id} />
                <h3>{me.profile.name}</h3>
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

              <DropdownMenuItem onClick={() => navigate("/chats")}>
                <ListIcon />
                Chat List
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          <div className="ms-auto">
            {isAuthenticated ? (
              <Button variant="ghost" onClick={() => logOut()}>
                Log out
              </Button>
            ) : (
              <AuthModal />
            )}
          </div>
        </>
      )}
    </>
  );
}
