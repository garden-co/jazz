"use client";

import { useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent, type Ref } from "react";

export type InlineTextVariant = "title" | "heading" | "body";

/**
 * A borderless text area that reads as the text it edits, for page titles and
 * block text. Astryx has no inline-editing primitive, so this is the one local
 * control (see globals.css and the PR's design-system gaps).
 *
 * It keeps a local copy while you type, reports each change together with the
 * value it was based on (so callers can send a minimal splice), and adopts a
 * collaborator's change as soon as the synced value moves away from that base.
 */
export function InlineText({
  value,
  label,
  variant = "body",
  placeholder,
  isReadOnly = false,
  hasAutoFocus = false,
  isChecked = false,
  inputRef,
  onChange,
  onKeyDown,
}: {
  value: string;
  label: string;
  variant?: InlineTextVariant;
  placeholder?: string;
  isReadOnly?: boolean;
  hasAutoFocus?: boolean;
  isChecked?: boolean;
  inputRef?: Ref<HTMLTextAreaElement>;
  onChange?: (next: string, base: string) => void;
  onKeyDown?: (event: KeyboardEvent<HTMLTextAreaElement>) => void;
}) {
  const [text, setText] = useState(value);
  const base = useRef(value);
  const element = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    if (value === base.current) return;
    base.current = value;
    setText(value);
  }, [value]);

  // Browsers without `field-sizing: content` grow the area by hand.
  useLayoutEffect(() => {
    const area = element.current;
    if (!area || CSS.supports("field-sizing", "content")) return;
    area.style.height = "auto";
    area.style.height = `${area.scrollHeight}px`;
  }, [text]);

  return (
    <textarea
      ref={(node) => {
        element.current = node;
        if (typeof inputRef === "function") inputRef(node);
        else if (inputRef) inputRef.current = node;
      }}
      className="bb-inline-text"
      data-variant={variant}
      data-checked={isChecked ? "true" : undefined}
      aria-label={label}
      rows={1}
      value={text}
      placeholder={isReadOnly ? undefined : placeholder}
      readOnly={isReadOnly}
      autoFocus={hasAutoFocus}
      spellCheck
      onChange={(event) => {
        const next = event.target.value;
        const previous = base.current;
        base.current = next;
        setText(next);
        onChange?.(next, previous);
      }}
      onKeyDown={onKeyDown}
    />
  );
}
