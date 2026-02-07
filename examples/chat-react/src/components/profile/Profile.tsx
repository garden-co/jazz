import { Suspense, useState } from "react";
import {
  useIsAuthenticated,
  useLogOut,
  usePasskeyAuth,
  usePassphraseAuth,
  useSuspenseAccount,
} from "jazz-tools/react";
import { FingerprintIcon, LogOutIcon } from "lucide-react";
import { toast } from "sonner";
import { Avatar } from "@/components/Avatar";
import { ProfilePassphrase } from "@/components/profile/ProfilePassphrase";
import { Button } from "@/components/ui/button";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldSet,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { wordlist } from "@/lib/wordlist";
import { ChatAccountWithProfile } from "@/schema";

interface ProfileProps {
  onClose: () => void;
  children: React.ReactNode;
}

export function Profile({ onClose, children }: ProfileProps) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet
      onOpenChange={(v) => {
        setOpen(v);
        if (!v) onClose?.();
      }}
      open={open}
    >
      <SheetTrigger asChild>{children}</SheetTrigger>
      <SheetContent className="overflow-y-auto">
        <Suspense
          fallback={
            <div className="p-8 text-center text-muted-foreground italic">
              Loading account...
            </div>
          }
        >
          <ProfileContent setOpen={setOpen} />
        </Suspense>
      </SheetContent>
    </Sheet>
  );
}

function ProfileContent({ setOpen }: { setOpen: (v: boolean) => void }) {
  const me = useSuspenseAccount(ChatAccountWithProfile);
  const passphraseAuth = usePassphraseAuth({
    wordlist,
  });
  const passkeyAuth = usePasskeyAuth({
    appName: "Jazz Chat",
  });
  const logOut = useLogOut();
  const isAuthenticated = useIsAuthenticated();

  return (
    <>
      <SheetHeader>
        <SheetTitle>Your Account</SheetTitle>
      </SheetHeader>

      <SheetDescription className="hidden">
        Manage your account settings and preferences.
      </SheetDescription>
      <div className="px-4">
        <FieldSet>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="name">Name</FieldLabel>
              <FieldDescription>
                The name you would like to be known by.
              </FieldDescription>
              <Input
                type="text"
                id="name"
                value={me.profile.name}
                onChange={(evt) => {
                  me.profile.$jazz.set("name", evt.currentTarget.value);
                }}
              />
            </Field>
            <Avatar editable />
            <Field>
              <FieldLabel htmlFor="passkey-register">Passkey</FieldLabel>
              <FieldDescription>
                A passkey allows you to log back in quickly and easily.
              </FieldDescription>
              <Button
                id="passkey-register"
                onClick={async () => {
                  await passkeyAuth.signUp(me.profile.name);
                  setOpen(false);
                }}
              >
                <FingerprintIcon /> Register a passkey
              </Button>
            </Field>
            <ProfilePassphrase />
            {!isAuthenticated && (
              <Button
                variant="secondary"
                onClick={async () => {
                  await passphraseAuth.signUp();
                  toast.success("You're authenticated!");
                  setOpen(false);
                }}
              >
                I've saved my passphrase
              </Button>
            )}
          </FieldGroup>
        </FieldSet>
      </div>
      <SheetFooter>
        <Field>
          <FieldLabel htmlFor="logout">Log Out</FieldLabel>
          <FieldDescription>
            If you log out, you will be automatically provisioned with a new
            anonymous account.
          </FieldDescription>
          <Button
            id="logout"
            variant="destructive"
            onClick={() => {
              logOut();
              setOpen(false);
            }}
          >
            <LogOutIcon /> Log Out
          </Button>
        </Field>
      </SheetFooter>
    </>
  );
}
