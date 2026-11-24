import { useRef, useState, type FormEvent } from "react";
import { useDb } from "jazz-tools/react";
import {
  Banner,
  Button,
  Dialog,
  DialogHeader,
  HStack,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { app, type Profile } from "../../jazz.generated";

export function NewRoomDialog({
  isOpen,
  onOpenChange,
  author,
  profile,
  onCreated,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  author: string;
  profile: Profile;
  onCreated: (roomId: string) => void;
}) {
  const db = useDb();
  const creating = useRef(false);
  const [busy, setBusy] = useState(false);
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || creating.current) return;
    creating.current = true;
    setBusy(true);
    try {
      setError(null);
      // #region bandchat-create-room
      // The room is its own encrypted space. Its creator's member row names
      // the room, so the room's id is chosen first and Jazz fills in the
      // rest of the row's address.
      const roomId = crypto.randomUUID();
      let creatorMember: Awaited<ReturnType<typeof db.encryption.members.row>>;
      try {
        creatorMember = await db.encryption.members.row({ table: "rooms", id: roomId }, author);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
        return;
      }
      // Local-first: the room is usable before the server confirms it. The
      // room, its creator's member row and profile membership commit together;
      // the checks on the memberships see the room inserted earlier in the
      // same transaction.
      //
      // This stages and commits by hand instead of using `db.transaction`,
      // which returns a Promise: the dialog must close and select the room in
      // the same step as the commit, before the new room renders, or it would
      // render under the still-open modal.
      const tx = db.beginTransaction();
      try {
        tx.insert(app.rooms, { name: trimmed }, { id: roomId });
        tx.insert(app.__e2ee_members, creatorMember);
        tx.insert(app.roomMembers, {
          roomId,
          memberAuthor: author,
          memberProfileId: profile.id,
        });
      } catch (cause) {
        void tx.rollback().catch(() => {});
        setError(cause instanceof Error ? cause.message : String(cause));
        return;
      }
      let committed: ReturnType<typeof tx.commit>;
      try {
        committed = tx.commit();
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
        return;
      }
      setName("");
      onOpenChange(false);
      onCreated(roomId);
      // If the server rejects the room, it disappears; reopen the dialog with
      // the name and the reason so the creator can see what happened.
      committed.wait({ tier: "global" }).catch((cause: unknown) => {
        setName(trimmed);
        setError(
          `Could not create the room: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
        onOpenChange(true);
      });
      // #endregion bandchat-create-room
    } finally {
      creating.current = false;
      setBusy(false);
    }
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form">
      <DialogHeader title="New room" onOpenChange={onOpenChange} />
      <form onSubmit={(event) => void create(event)}>
        <VStack gap={4} padding={4}>
          <TextInput
            label="Room name"
            placeholder="Rehearsal"
            value={name}
            onChange={setName}
            hasAutoFocus
            isRequired
          />
          {error ? (
            <Banner status="error" title={error} container="section" collapsible={false} />
          ) : null}
          <HStack gap={2} justify="end">
            <Button label="Cancel" variant="ghost" onClick={() => onOpenChange(false)} />
            <Button label="Create room" variant="primary" type="submit" isDisabled={busy} />
          </HStack>
        </VStack>
      </form>
    </Dialog>
  );
}
