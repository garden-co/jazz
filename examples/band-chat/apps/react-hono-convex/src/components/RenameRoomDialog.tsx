import { useState, type FormEvent } from "react";
import { useMutation } from "convex/react";
import { Button, Dialog, DialogHeader, HStack, TextInput, VStack } from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Id } from "../lib/model";

export function RenameRoomDialog({
  isOpen,
  onOpenChange,
  roomId,
  name,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  roomId: Id<"rooms">;
  name: string;
}) {
  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form">
      <DialogHeader title="Rename room" onOpenChange={onOpenChange} />
      {isOpen ? (
        <RenameForm roomId={roomId} name={name} onDone={() => onOpenChange(false)} />
      ) : null}
    </Dialog>
  );
}

function RenameForm({
  roomId,
  name,
  onDone,
}: {
  roomId: Id<"rooms">;
  name: string;
  onDone: () => void;
}) {
  const rename = useMutation(api.rooms.rename);
  const [value, setValue] = useState(name);
  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) return;
    void rename({ roomId, name: trimmed });
    onDone();
  }
  return (
    <form onSubmit={save}>
      <VStack gap={4} padding={4}>
        <TextInput label="Room name" value={value} onChange={setValue} hasAutoFocus isRequired />
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onDone} />
          <Button label="Save" variant="primary" type="submit" />
        </HStack>
      </VStack>
    </form>
  );
}
