import { useState } from "react";
import { usePasskeyAuth, usePassphraseAuth } from "jazz-tools/react";
import { FingerprintIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldSet,
} from "@/components/ui/field";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { wordlist } from "@/lib/wordlist";

export const AuthModal = () => {
  const passphraseAuth = usePassphraseAuth({
    wordlist,
  });
  const passkeyAuth = usePasskeyAuth({
    appName: "Jazz Chat",
  });
  const [passphrase, setPassphrase] = useState("");

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="ghost">Log In</Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Log In</DialogTitle>
        </DialogHeader>
        <Tabs defaultValue="passkey">
          <TabsList>
            <TabsTrigger value="passkey">Passkey</TabsTrigger>
            <TabsTrigger value="passphrase">Passphrase</TabsTrigger>
          </TabsList>
          <TabsContent value="passkey">
            <Field>
              <FieldLabel htmlFor="passkey-login">Passkey</FieldLabel>
              <FieldDescription>
                Passkeys are a secure way to log in without a password. If you
                have previously registered a passkey, you can use it to log in.
              </FieldDescription>
              <Button
                id="passkey-login"
                onClick={() => {
                  passkeyAuth.logIn();
                }}
                className="justify-self-end"
              >
                <FingerprintIcon /> Log in using a passkey
              </Button>
            </Field>
          </TabsContent>
          <TabsContent value="passphrase">
            <FieldSet>
              <FieldGroup>
                <Field>
                  <FieldLabel htmlFor="passphrase">Passphrase</FieldLabel>
                  <FieldDescription>
                    If you have a passphrase, you can enter it below.
                  </FieldDescription>

                  <Textarea
                    value={passphrase}
                    onChange={(evt) => setPassphrase(evt.currentTarget.value)}
                    rows={3}
                  />
                </Field>
                <Field>
                  <Button
                    onClick={() => {
                      passphraseAuth.logIn(passphrase);
                    }}
                  >
                    Log in using passphrase
                  </Button>
                </Field>
              </FieldGroup>
            </FieldSet>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
};
