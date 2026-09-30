import * as React from "react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { TextInput } from "@astryxdesign/core/TextInput";

interface NameDialogProps {
  isOpen: boolean;
  title: string;
  label: string;
  initialValue: string;
  actionLabel: string;
  onSubmit: (name: string) => void;
  onClose: () => void;
}

/** Asks for one name: new folders and renames. */
export function NameDialog({
  isOpen,
  title,
  label,
  initialValue,
  actionLabel,
  onSubmit,
  onClose,
}: NameDialogProps) {
  const [name, setName] = React.useState(initialValue);
  React.useEffect(() => {
    if (isOpen) setName(initialValue);
  }, [isOpen, initialValue]);
  const trimmed = name.trim();
  const submit = () => {
    if (!trimmed) return;
    onSubmit(trimmed);
    onClose();
  };
  return (
    <Dialog isOpen={isOpen} onOpenChange={(open) => !open && onClose()} width={440} purpose="form">
      <VStack gap={4}>
        <DialogHeader title={title} onOpenChange={(open) => !open && onClose()} />
        <TextInput
          label={label}
          value={name}
          onChange={setName}
          onKeyDown={(event) => {
            // Handle Enter here so the key press does not also activate the
            // button that gets focus back when the dialog closes.
            if (event.key !== "Enter" || event.nativeEvent.isComposing) return;
            event.preventDefault();
            submit();
          }}
          hasAutoFocus
        />
        <HStack gap={2} hAlign="end">
          <Button label="Cancel" variant="secondary" onClick={onClose} />
          <Button label={actionLabel} variant="primary" isDisabled={!trimmed} onClick={submit} />
        </HStack>
      </VStack>
    </Dialog>
  );
}
