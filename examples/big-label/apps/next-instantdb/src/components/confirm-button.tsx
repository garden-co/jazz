"use client";

import { useState } from "react";
import { AlertDialog, Button } from "@astryxdesign/core";

/** A destructive button that asks before it acts. */
export function ConfirmButton({
  label,
  title,
  description,
  onConfirm,
}: {
  label: string;
  title: string;
  description: string;
  onConfirm: () => void;
}) {
  const [isOpen, setIsOpen] = useState(false);
  return (
    <>
      <Button label={label} variant="destructive" onClick={() => setIsOpen(true)} />
      <AlertDialog
        isOpen={isOpen}
        onOpenChange={setIsOpen}
        title={title}
        description={description}
        actionLabel={label}
        actionVariant="destructive"
        onAction={() => {
          setIsOpen(false);
          onConfirm();
        }}
      />
    </>
  );
}
