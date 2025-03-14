export { useSecretURLAuth } from "jazz-react-core";
import { ID, SecretURLAuthTransfer } from "jazz-tools";
import { useAcceptInvite } from "../hooks.js";

export function useAcceptAuthInvite({
  onAccept,
}: { onAccept: (id: ID<SecretURLAuthTransfer>) => void }) {
  return useAcceptInvite({
    invitedObjectSchema: SecretURLAuthTransfer,
    forValueHint: "authTransfer",
    onAccept,
  });
}
