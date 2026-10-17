import { useId, type FormEvent, type ReactNode } from "react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Layout, LayoutContent, LayoutFooter } from "@astryxdesign/core/Layout";
import { HStack, VStack } from "@astryxdesign/core/Stack";

type FormDialogProps = {
  title: string;
  submitLabel: string;
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  onSubmit: () => void | Promise<void>;
  isSubmitDisabled?: boolean;
  children: ReactNode;
};

/** A dialog around a form: fields in the body, cancel and submit in the footer. */
export function FormDialog({
  title,
  submitLabel,
  isOpen,
  onOpenChange,
  onSubmit,
  isSubmitDisabled,
  children,
}: FormDialogProps) {
  const formId = useId();
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (isSubmitDisabled) return;
    await onSubmit();
    onOpenChange(false);
  };
  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form" width={480}>
      <Layout
        header={<DialogHeader title={title} onOpenChange={onOpenChange} />}
        content={
          <LayoutContent>
            <form id={formId} onSubmit={submit}>
              <VStack gap={4}>{children}</VStack>
            </form>
          </LayoutContent>
        }
        footer={
          <LayoutFooter hasDivider>
            <HStack gap={2} hAlign="end">
              <Button label="Cancel" variant="ghost" onClick={() => onOpenChange(false)} />
              <Button
                label={submitLabel}
                variant="primary"
                type="submit"
                form={formId}
                isDisabled={isSubmitDisabled}
              />
            </HStack>
          </LayoutFooter>
        }
      />
    </Dialog>
  );
}
