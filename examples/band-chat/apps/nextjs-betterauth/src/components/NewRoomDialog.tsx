"use client";

import { useState, type FormEvent } from "react";
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
import { app, type Profile } from "../../schema";

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
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setError(null);
    try {
      // Both writes are local-first: the room is usable before the server
      // confirms them. The creator's own membership is the bootstrap step
      // the room policy allows only for the creator.
      //
      // They are deliberately two writes, not one transaction: the membership
      // policy's `exists` check on the room only sees committed rows (INV-RLS-9
      // in the Jazz authorization spec; garden-co/jazz#3755), so a membership
      // staged in the same transaction as its room would be rejected. Once
      // #3755 is fixed they become one transaction. Until then, if the
      // membership write is rejected, the creator still sees the room (they
      // can read it as its creator) and the room view offers them "Join room".
      const room = db.insert(app.rooms, { name: trimmed }).value;
      db.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: author,
        memberProfileId: profile.id,
      });
      setName("");
      onOpenChange(false);
      onCreated(room.id);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form">
      <DialogHeader title="New room" onOpenChange={onOpenChange} />
      <form onSubmit={create}>
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
            <Button label="Create room" variant="primary" type="submit" />
          </HStack>
        </VStack>
      </form>
    </Dialog>
  );
}
