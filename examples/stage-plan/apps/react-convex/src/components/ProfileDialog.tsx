import { useState } from "react";
import { api } from "../../convex/_generated/api.js";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import type { Me } from "../model/actions.js";
import { useWrite } from "../model/write.js";
import { FormDialog } from "./FormDialog.js";

type ProfileDialogProps = { me: Me; isOpen: boolean; onOpenChange: (isOpen: boolean) => void };

export function ProfileDialog({ me, isOpen, onOpenChange }: ProfileDialogProps) {
  const rename = useWrite(api.crew.rename);
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
        void rename({ name: name.trim() });
      }}
    >
      <Text color="secondary">
        Crew on your shows see this name on tasks, comments and the activity feed.
      </Text>
      <TextInput label="Name" value={name} onChange={setName} hasAutoFocus />
    </FormDialog>
  );
}
