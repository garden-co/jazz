import { useState } from "react";
import { usePassphraseAuth } from "jazz-tools/react";
import { EyeIcon, EyeOffIcon } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Textarea } from "@/components/ui/textarea";
import { wordlist } from "@/lib/wordlist";

export function ProfilePassphrase() {
  const passphraseAuth = usePassphraseAuth({ wordlist });
  const [revealPassphrase, setRevealPassphrase] = useState(false);

  return (
    <Field>
      <FieldLabel htmlFor="passphrase">Passphrase</FieldLabel>
      <FieldDescription>
        A secret code you can use to recover your account.
      </FieldDescription>
      <Button variant="outline" onClick={() => setRevealPassphrase((v) => !v)}>
        {revealPassphrase ? (
          <>
            <EyeOffIcon /> Hide
          </>
        ) : (
          <>
            <EyeIcon />
            Reveal
          </>
        )}
      </Button>
      <Textarea
        value={
          revealPassphrase
            ? passphraseAuth.passphrase
            : passphraseAuth.passphrase.replace(/\S/g, "*")
        }
        disabled={!revealPassphrase}
        className={`${
          revealPassphrase && "cursor-copy!"
        }  caret-transparent focus:ring-0 text-muted-foreground`}
        onClick={() => {
          navigator.clipboard.writeText(passphraseAuth.passphrase);
          toast.success("Passphrase copied to clipboard!");
        }}
        readOnly
        rows={4}
      />
    </Field>
  );
}
