"use client";

import { useRef, useState } from "react";
import { ChatComposer, ChatComposerDrawer } from "@astryxdesign/core/Chat";
import { Button } from "@astryxdesign/core/Button";
import { Token } from "@astryxdesign/core/Token";

export function Composer({
  isReplying,
  error,
  placeholder,
  onSend,
}: {
  isReplying: boolean;
  error?: string;
  placeholder: string;
  onSend: (text: string, files: File[]) => void;
}) {
  const [draft, setDraft] = useState("");
  const [files, setFiles] = useState<File[]>([]);
  const picker = useRef<HTMLInputElement>(null);

  return (
    <ChatComposer
      value={draft}
      onChange={setDraft}
      placeholder={placeholder}
      isDisabled={isReplying}
      onSubmit={(value) => {
        const text = value.trim();
        if (!text && !files.length) return;
        onSend(text, files);
        setDraft("");
        setFiles([]);
      }}
      status={error ? { type: "error", message: error } : undefined}
      headerActions={
        <>
          <Button
            size="sm"
            variant="ghost"
            label="Attach audio"
            onClick={() => picker.current?.click()}
            isDisabled={isReplying}
          />
          <input
            ref={picker}
            type="file"
            accept="audio/*"
            multiple
            hidden
            onChange={(event) => {
              setFiles((current) => [...current, ...Array.from(event.target.files ?? [])]);
              event.target.value = "";
            }}
          />
        </>
      }
      drawer={
        files.length > 0 ? (
          <ChatComposerDrawer>
            {files.map((file, index) => (
              <Token
                key={`${file.name}-${index}`}
                label={file.name}
                onRemove={() => setFiles((current) => current.filter((_, i) => i !== index))}
              />
            ))}
          </ChatComposerDrawer>
        ) : undefined
      }
    />
  );
}
