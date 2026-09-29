import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app } from "../../schema.js";
import type { Me } from "../data/actions.js";
import { FormDialog } from "./FormDialog.js";

type ProfileDialogProps = { me: Me; isOpen: boolean; onOpenChange: (isOpen: boolean) => void };

export function ProfileDialog({ me, isOpen, onOpenChange }: ProfileDialogProps) {
  const db = useDb();
  const [name, setName] = useState(me.profile.name);
  const [wasOpen, setWasOpen] = useState(isOpen);
  if (isOpen !== wasOpen) {
    setWasOpen(isOpen);
    if (isOpen) setName(me.profile.name);
  }

  return (
    <FormDialog
      title="Your name"
      submitLabel="Save"
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      isSubmitDisabled={!name.trim()}
      onSubmit={() => {
        db.update(app.crew, me.profile.id, { name: name.trim() });
      }}
    >
      <Text color="secondary">
        Crew on your shows see this name on tasks, comments and the activity feed.
      </Text>
      <TextInput label="Name" value={name} onChange={setName} hasAutoFocus />
    </FormDialog>
  );
}
