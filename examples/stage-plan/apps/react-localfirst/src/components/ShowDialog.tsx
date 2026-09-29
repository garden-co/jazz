import { useState } from "react";
import { DateInput, type DateInputProps } from "@astryxdesign/core/DateInput";
import { Grid } from "@astryxdesign/core/Grid";
import { TextInput } from "@astryxdesign/core/TextInput";
import { TimeInput, type TimeInputProps } from "@astryxdesign/core/TimeInput";
import type { ShowInput } from "../data/actions.js";
import { FormDialog } from "./FormDialog.js";

type ShowDialogProps = {
  title: string;
  submitLabel: string;
  initial?: ShowInput;
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  onSubmit: (input: ShowInput) => void | Promise<void>;
};

const EMPTY: ShowInput = { name: "", venue: "", date: "", doors: "19:00" };

/** Create or edit a show's name, venue, date and doors time. */
export function ShowDialog({
  title,
  submitLabel,
  initial,
  isOpen,
  onOpenChange,
  onSubmit,
}: ShowDialogProps) {
  const [draft, setDraft] = useState<ShowInput>(initial ?? EMPTY);
  // Start from the current values each time the dialog opens.
  const [wasOpen, setWasOpen] = useState(isOpen);
  if (isOpen !== wasOpen) {
    setWasOpen(isOpen);
    if (isOpen) setDraft(initial ?? EMPTY);
  }
  const set = (patch: Partial<ShowInput>) => setDraft((current) => ({ ...current, ...patch }));
  const isComplete = Boolean(draft.name.trim() && draft.venue.trim() && draft.date && draft.doors);

  return (
    <FormDialog
      title={title}
      submitLabel={submitLabel}
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      isSubmitDisabled={!isComplete}
      onSubmit={() => onSubmit({ ...draft, name: draft.name.trim(), venue: draft.venue.trim() })}
    >
      <TextInput
        label="Show"
        placeholder="Headliner, tour or event"
        value={draft.name}
        onChange={(name) => set({ name })}
        hasAutoFocus
        isRequired
      />
      <TextInput
        label="Venue"
        value={draft.venue}
        onChange={(venue) => set({ venue })}
        isRequired
      />
      <Grid columns={{ minWidth: 160 }} gap={4}>
        <DateInput
          label="Date"
          value={(draft.date || undefined) as DateInputProps["value"]}
          onChange={(date) => set({ date: date ?? "" })}
          isRequired
        />
        <TimeInput
          label="Doors"
          value={(draft.doors || undefined) as TimeInputProps["value"]}
          onChange={(doors) => set({ doors: doors ?? "" })}
          hourFormat="24h"
          isRequired
        />
      </Grid>
    </FormDialog>
  );
}
