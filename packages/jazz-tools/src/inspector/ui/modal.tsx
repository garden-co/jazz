import { styled } from "goober";
import { forwardRef, useEffect, useRef } from "react";
import { Button } from "./button.js";
import { Heading } from "./heading.js";

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  heading: string;
  text?: string;
  children?: React.ReactNode;
  confirmText?: string;
  cancelText?: string;
  onConfirm?: () => void;
  onCancel?: () => void;
  showButtons?: boolean;
  className?: string;
}

const ModalContent = styled("dialog")`
  background-color: var(--j-background);
  border-radius: var(--j-radius-lg);
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
  border: 1px solid var(--j-border-color);
  max-width: 32rem;
  margin-block: auto;
  margin-inline: auto;
  &::backdrop {
    background-color: rgba(0, 0, 0, 0.7);
  }

`;

const ModalHeader = styled("div")`
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 1.5rem 1.5rem 0 1.5rem;
  gap: 1rem;
`;

const ModalBody = styled("div")`
  padding: 1rem 1.5rem;
  flex: 1;
`;

const ModalFooter = styled("div")`
  display: flex;
  justify-content: flex-end;
  gap: 0.75rem;
  padding: 0 1.5rem 1.5rem 1.5rem;
`;

const CloseButton = styled("button")`
  background: none;
  border: none;
  cursor: pointer;
  padding: 0.25rem;
  border-radius: var(--j-radius-sm);
  color: var(--j-text-color);
  font-size: 1.25rem;
  line-height: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  min-width: 2rem;
  min-height: 2rem;

  &:hover {
    background-color: var(--j-foreground);
  }

  &:focus-visible {
    outline: 2px solid var(--j-border-focus);
    outline-offset: 2px;
  }
`;

export const Modal = forwardRef<HTMLDialogElement, ModalProps>(
  (
    {
      isOpen,
      onClose,
      heading,
      text,
      children,
      confirmText = "Confirm",
      cancelText = "Cancel",
      onConfirm,
      onCancel,
      showButtons = true,
      className,
    },
    ref,
  ) => {
    const modalRef = useRef<HTMLDialogElement>(null);

    useEffect(() => {
      if (isOpen) {
        modalRef.current?.showModal();
      } else {
        onClose();
        modalRef.current?.close();
      }
    }, [isOpen, onClose]);

    const handleConfirm = () => {
      onConfirm?.();
      onClose();
    };

    const handleCancel = () => {
      onCancel?.();
      onClose();
    };

    if (!isOpen) return null;

    return (
      <ModalContent
        ref={ref || modalRef}
        className={className}
        role="dialog"
        aria-labelledby="modal-heading"
        onClose={onClose}
      >
        <ModalHeader>
          <Heading id="modal-heading">{heading}</Heading>
          <CloseButton onClick={onClose} aria-label="Close modal" type="button">
            ×
          </CloseButton>
        </ModalHeader>

        <ModalBody>
          {text && (
            <p style={{ margin: "0 0 1rem 0", color: "var(--j-text-color)" }}>
              {text}
            </p>
          )}
          {children}
        </ModalBody>

        {showButtons && (
          <ModalFooter>
            <Button variant="secondary" onClick={handleCancel}>
              {cancelText}
            </Button>
            <Button variant="primary" onClick={handleConfirm}>
              {confirmText}
            </Button>
          </ModalFooter>
        )}
      </ModalContent>
    );
  },
);

Modal.displayName = "Modal";
