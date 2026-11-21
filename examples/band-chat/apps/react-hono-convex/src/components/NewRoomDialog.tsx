import { useState, type FormEvent } from "react";
import { useMutation } from "convex/react";
import {
  Banner,
  Button,
  Dialog,
  DialogHeader,
  HStack,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Profile } from "../lib/model";

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
  const createRoom = useMutation(api.rooms.create);
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setError(null);
    // The room and its creator's membership commit together (one mutation).
    // The dialog closes once the server has created the room.
    try {
      const roomId = await createRoom({ name: trimmed });
      setName("");
      onOpenChange(false);
      onCreated(roomId);
    } catch (cause) {
      setError(
        `Could not create the room: ${cause instanceof Error ? cause.message : String(cause)}`,
      );
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
            <Button label="Create room" variant="primary" type="submit" />
          </HStack>
        </VStack>
      </form>
    </Dialog>
  );
}
