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
    // Local-first: the room is usable before the server confirms it. The
    // room and its creator's membership commit together; the membership is
    // the bootstrap step the room policy allows only for the creator, and its
    // `exists` check sees the room inserted earlier in the same transaction.
    db.transaction((tx) => {
      const room = tx.insert(app.rooms, { name: trimmed });
      tx.insert(app.roomMembers, {
        roomId: room.id,
        memberAuthor: author,
        memberProfileId: profile.id,
      });
      return room;
    })
      .then((created) => {
        setName("");
        onOpenChange(false);
        onCreated(created.value.id);
      })
      .catch((cause: unknown) => {
        setError(cause instanceof Error ? cause.message : String(cause));
      });
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
