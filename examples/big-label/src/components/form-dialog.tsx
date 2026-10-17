"use client";

import type { FormEvent, ReactNode } from "react";
import {
  Button,
  Dialog,
  DialogHeader,
  HStack,
  Layout,
  LayoutContent,
  LayoutFooter,
  VStack,
} from "@astryxdesign/core";

/** A dialog holding one form: fields in the body, cancel and submit below. */
export function FormDialog({
  title,
  isOpen,
  onOpenChange,
  submitLabel,
  onSubmit,
  canSubmit = true,
  children,
}: {
  title: string;
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  submitLabel: string;
  /** Return `false` to keep the dialog open, for example after a validation error. */
  onSubmit: () => boolean | void;
  canSubmit?: boolean;
  children: ReactNode;
}) {
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!canSubmit) return;
    if (onSubmit() !== false) onOpenChange(false);
  };
  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form" width={480}>
      <form onSubmit={submit}>
        <Layout
          header={<DialogHeader title={title} onOpenChange={onOpenChange} />}
          content={
            <LayoutContent>
              <VStack gap={4}>{children}</VStack>
            </LayoutContent>
          }
          footer={
            <LayoutFooter hasDivider>
              <HStack gap={2} justify="end">
                <Button label="Cancel" variant="secondary" onClick={() => onOpenChange(false)} />
                <Button label={submitLabel} type="submit" isDisabled={!canSubmit} />
              </HStack>
            </LayoutFooter>
          }
        />
      </form>
    </Dialog>
  );
}
