import { useSecretURLAuth as useSecretURLAuthCore } from "jazz-react-core";
import { ID, SecretURLAuthTransfer } from "jazz-tools";
import { useAcceptInvite } from "../hooks.js";

export function useSecretURLAuth() {
  return useSecretURLAuthCore(window.location.origin);
}

export function useAcceptAuthInvite({
  onAccept,
}: { onAccept: (id: ID<SecretURLAuthTransfer>) => void }) {
  return useAcceptInvite({
    invitedObjectSchema: SecretURLAuthTransfer,
    forValueHint: "authTransfer",
    onAccept,
  });
}
