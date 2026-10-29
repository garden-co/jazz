"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useAll, useDb } from "jazz-tools/react";
import {
  Banner,
  Blockquote,
  CheckboxInput,
  Divider,
  DropdownMenu,
  HStack,
  List,
  ListItem,
  MoreMenu,
  Skeleton,
  VStack,
  type DropdownMenuOption,
} from "@astryxdesign/core";
import { app, type Block, type BlockKind } from "@/schema";
import { comparePositioned, positionBetween } from "@/src/lib/positions";
import { AttachmentBlock } from "./AttachmentBlock";
import { RichText, type RichTextHandle, type RichTextKeyInfo } from "./RichText";
import { UploadDialog } from "./UploadDialog";
import { useWorkspace } from "./workspace-context";

type Row = Block & { $createdAt?: Date | number | null };

const TEXT_KINDS: { kind: BlockKind; label: string }[] = [
  { kind: "paragraph", label: "Text" },
  { kind: "heading", label: "Heading" },
  { kind: "todo", label: "To-do" },
  { kind: "bullet", label: "Bulleted list" },
  { kind: "quote", label: "Quote or lyrics" },
];

const PLACEHOLDERS: Partial<Record<BlockKind, string>> = {
  paragraph: "Write something, or type # for a heading, - for a list, [] for a to-do",
  heading: "Heading",
  todo: "To-do",
  bullet: "List item",
  quote: "Lyrics or a quote. Shift+Enter for a new line",
};

/** Typing one of these at the start of a text block turns it into another kind. */
const SHORTCUTS: [prefix: string, kind: BlockKind][] = [
  ["# ", "heading"],
  ["- ", "bullet"],
  ["* ", "bullet"],
  ["[] ", "todo"],
  ["[ ] ", "todo"],
  ["> ", "quote"],
];

/**
 * An ordered, nested list of blocks. Every edit is a direct Jazz write: it is
 * visible immediately, survives going offline, and reaches everyone else with
 * access to the page as it syncs. A block's text is inline rich text stored as
 * Markdown in a text merge column, so two people editing the same block keep
 * both edits.
 */
export function BlockEditor({ pageId, editable }: { pageId: string; editable: boolean }) {
  const db = useDb();
  const { workspace } = useWorkspace();
  const { data: blocks } = useAll(app.blocks.where({ pageId }).select("*", "$createdAt"));
  const [uploading, setUploading] = useState(false);
  const [writeError, setWriteError] = useState<string | null>(null);
  const inputs = useRef(new Map<string, RichTextHandle>());
  const [focusRequest, setFocusRequest] = useState<{ id: string; at: "start" | "end" } | null>(
    null,
  );

  const { childrenOf, flat } = useMemo(() => {
    const byParent = new Map<string | null, Row[]>();
    for (const block of (blocks ?? []) as Row[]) {
      const key = block.parentBlockId ?? null;
      byParent.set(key, [...(byParent.get(key) ?? []), block]);
    }
    for (const list of byParent.values()) list.sort(comparePositioned);
    const childrenOf = (id: string | null) => byParent.get(id) ?? [];
    const flat: Row[] = [];
    const walk = (id: string | null) =>
      childrenOf(id).forEach((block) => {
        flat.push(block);
        walk(block.id);
      });
    walk(null);
    return { childrenOf, flat };
  }, [blocks]);

  useEffect(() => {
    if (!focusRequest) return;
    const input = inputs.current.get(focusRequest.id);
    if (!input) return;
    input.focus(focusRequest.at);
    setFocusRequest(null);
  }, [focusRequest, blocks]);

  if (!blocks)
    return (
      <VStack gap={2}>
        <Skeleton height={20} width="80%" />
        <Skeleton height={20} width="60%" />
      </VStack>
    );

  const siblingsOf = (block: Row) => childrenOf(block.parentBlockId ?? null);

  const insertBlock = (
    kind: BlockKind,
    placement: { after?: Row; parentBlockId?: string | null },
    text = "",
  ) => {
    const parentBlockId = placement.after
      ? (placement.after.parentBlockId ?? null)
      : (placement.parentBlockId ?? null);
    const siblings = childrenOf(parentBlockId);
    let position: number;
    if (placement.after) {
      const index = siblings.findIndex((sibling) => sibling.id === placement.after!.id);
      position = positionBetween(placement.after.position, siblings[index + 1]?.position);
    } else {
      position = positionBetween(siblings.at(-1)?.position, undefined);
    }
    const inserted = db.insert(app.blocks, {
      workspaceId: workspace.id,
      pageId,
      parentBlockId,
      position,
      kind,
      text,
      checked: false,
      attachmentId: null,
    }).value;
    setFocusRequest({ id: inserted.id, at: "start" });
    return inserted;
  };

  const deleteBlock = (block: Row) => {
    const doomed: Row[] = [];
    const collect = (id: string) =>
      childrenOf(id).forEach((child) => {
        collect(child.id);
        doomed.push(child);
      });
    collect(block.id);
    doomed.push(block);
    // One transaction, so a block never loses its children without also going.
    setWriteError(null);
    db.transaction((tx) => {
      for (const row of doomed) tx.delete(app.blocks, row.id);
      if (block.attachmentId) tx.delete(app.attachments, block.attachmentId);
    }).catch((cause: unknown) =>
      setWriteError(cause instanceof Error ? cause.message : String(cause)),
    );
  };

  const indent = (block: Row) => {
    const siblings = siblingsOf(block);
    const previous = siblings[siblings.findIndex((sibling) => sibling.id === block.id) - 1];
    if (!previous) return;
    db.update(app.blocks, block.id, {
      parentBlockId: previous.id,
      position: positionBetween(childrenOf(previous.id).at(-1)?.position, undefined),
    });
    setFocusRequest({ id: block.id, at: "end" });
  };

  const outdent = (block: Row) => {
    if (!block.parentBlockId) return;
    const parent = flat.find((candidate) => candidate.id === block.parentBlockId);
    if (!parent) return;
    const parentSiblings = siblingsOf(parent);
    const after =
      parentSiblings[parentSiblings.findIndex((sibling) => sibling.id === parent.id) + 1];
    db.update(app.blocks, block.id, {
      parentBlockId: parent.parentBlockId ?? null,
      position: positionBetween(parent.position, after?.position),
    });
    setFocusRequest({ id: block.id, at: "end" });
  };

  const writeText = (block: Row, text: string) => {
    try {
      db.update(app.blocks, block.id, { text });
    } catch (cause) {
      // The block was deleted (here or by someone else) before the write.
      setWriteError(cause instanceof Error ? cause.message : String(cause));
    }
  };

  // Typing a Markdown-style prefix into an empty-ish paragraph turns it into
  // another kind of block, as in most block editors.
  const handleInput = (block: Row, plainText: string) => {
    if (block.kind !== "paragraph") return false;
    const input = inputs.current.get(block.id);
    if (!input) return false;
    if (plainText === "---") {
      input.takeLeading(3);
      db.update(app.blocks, block.id, { kind: "divider", text: "" });
      insertBlock("paragraph", { after: block });
      return true;
    }
    const shortcut = SHORTCUTS.find(([prefix]) => plainText.startsWith(prefix));
    if (!shortcut) return false;
    const text = input.takeLeading(shortcut[0].length);
    db.update(app.blocks, block.id, { kind: shortcut[1], text });
    setFocusRequest({ id: block.id, at: "start" });
    return true;
  };

  const handleKey = (block: Row, event: KeyboardEvent, caret: RichTextKeyInfo) => {
    const index = flat.findIndex((candidate) => candidate.id === block.id);
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      const continuesList = block.kind === "todo" || block.kind === "bullet";
      if (continuesList && caret.isEmpty) {
        // An empty list item ends the list, as in most editors.
        db.update(app.blocks, block.id, { kind: "paragraph" });
        return true;
      }
      insertBlock(continuesList ? block.kind : "paragraph", { after: block });
      return true;
    }
    if (event.key === "Backspace" && caret.isEmpty) {
      event.preventDefault();
      const previous = flat[index - 1];
      deleteBlock(block);
      if (previous) setFocusRequest({ id: previous.id, at: "end" });
      return true;
    }
    if (event.key === "Tab") {
      event.preventDefault();
      if (event.shiftKey) outdent(block);
      else indent(block);
      return true;
    }
    if (event.key === "ArrowUp" && caret.caretAtStart && index > 0) {
      event.preventDefault();
      setFocusRequest({ id: flat[index - 1].id, at: "end" });
      return true;
    }
    if (event.key === "ArrowDown" && caret.caretAtEnd && index < flat.length - 1) {
      event.preventDefault();
      setFocusRequest({ id: flat[index + 1].id, at: "start" });
      return true;
    }
    return false;
  };

  // Consecutive bullets share one list, so they read as a single list.
  const renderBlocks = (parentId: string | null) => {
    const out: ReactNode[] = [];
    let bullets: Row[] = [];
    const flushBullets = () => {
      if (bullets.length === 0) return;
      out.push(
        <List key={`list-${bullets[0].id}`} listStyle="disc" density="compact">
          {bullets.map((block) => (
            <ListItem key={block.id} label={renderBlock(block)} />
          ))}
        </List>,
      );
      bullets = [];
    };
    for (const block of childrenOf(parentId)) {
      if (block.kind === "bullet") bullets.push(block);
      else {
        flushBullets();
        out.push(renderBlock(block));
      }
    }
    flushBullets();
    return out;
  };

  const renderBlock = (block: Row): ReactNode => {
    const nested = childrenOf(block.id);
    const menu: DropdownMenuOption[] = [
      {
        type: "section",
        title: "Turn into",
        items: TEXT_KINDS.filter(({ kind }) => kind !== block.kind).map(({ kind, label }) => ({
          label,
          onClick: () => db.update(app.blocks, block.id, { kind }),
        })),
      },
      { type: "divider" },
      { label: "Indent", onClick: () => indent(block) },
      { label: "Outdent", onClick: () => outdent(block), isDisabled: !block.parentBlockId },
      { type: "divider" },
      { label: "Delete", variant: "destructive", onClick: () => deleteBlock(block) },
    ];
    return (
      <VStack key={block.id} gap={1}>
        <HStack gap={1} align="start" className="bb-block" data-block-kind={block.kind}>
          <VStack width="100%" minHeight={0}>
            <BlockContent
              block={block}
              editable={editable}
              registerInput={(handle) => {
                if (handle) inputs.current.set(block.id, handle);
                else inputs.current.delete(block.id);
              }}
              onWrite={(text) => writeText(block, text)}
              onInput={(plainText) => handleInput(block, plainText)}
              onKey={(event, caret) => handleKey(block, event, caret)}
              onToggle={(checked) => db.update(app.blocks, block.id, { checked })}
            />
          </VStack>
          {editable && (
            <span className="bb-block-menu">
              <MoreMenu
                label="Block actions"
                size="sm"
                alignment="end"
                items={
                  block.kind === "image" || block.kind === "file" || block.kind === "divider"
                    ? menu.slice(2)
                    : menu
                }
              />
            </span>
          )}
        </HStack>
        {nested.length > 0 && (
          <VStack gap={1} paddingInlineStart={6}>
            {renderBlocks(block.id)}
          </VStack>
        )}
      </VStack>
    );
  };

  return (
    <VStack gap={2}>
      {writeError && (
        <Banner
          status="error"
          title="Could not delete the block"
          description={writeError}
          collapsible={false}
        />
      )}
      <VStack gap={1}>{renderBlocks(null)}</VStack>
      {editable && (
        <HStack gap={2} wrap="wrap">
          <DropdownMenu
            button={{
              label: "Add block",
              variant: "ghost",
              size: "sm",
            }}
            items={[
              ...TEXT_KINDS.map(({ kind, label }) => ({
                label,
                onClick: () => insertBlock(kind, { parentBlockId: null }),
              })),
              { label: "Divider", onClick: () => insertBlock("divider", { parentBlockId: null }) },
              { type: "divider" as const },
              { label: "Image or file", onClick: () => setUploading(true) },
            ]}
          />
        </HStack>
      )}
      {uploading && (
        <UploadDialog
          pageId={pageId}
          position={positionBetween(childrenOf(null).at(-1)?.position, undefined)}
          onClose={() => setUploading(false)}
        />
      )}
    </VStack>
  );
}

function BlockContent({
  block,
  editable,
  registerInput,
  onWrite,
  onInput,
  onKey,
  onToggle,
}: {
  block: Row;
  editable: boolean;
  registerInput: (handle: RichTextHandle | null) => void;
  onWrite: (text: string) => void;
  onInput: (plainText: string) => boolean;
  onKey: (event: KeyboardEvent, caret: RichTextKeyInfo) => boolean;
  onToggle: (checked: boolean) => void;
}) {
  const text = (label: string, variant: "body" | "heading" = "body", isChecked = false) => (
    <RichText
      handleRef={registerInput}
      label={label}
      variant={variant}
      value={block.text}
      placeholder={PLACEHOLDERS[block.kind]}
      isReadOnly={!editable}
      isChecked={isChecked}
      onWrite={onWrite}
      onInput={onInput}
      onKeyDown={onKey}
    />
  );
  switch (block.kind) {
    case "heading":
      return text("Heading", "heading");
    case "todo":
      return (
        <HStack gap={2} align="start">
          <CheckboxInput
            label={block.text || "To-do"}
            isLabelHidden
            value={block.checked}
            isReadOnly={!editable}
            onChange={onToggle}
          />
          {text("To-do", "body", block.checked)}
        </HStack>
      );
    case "bullet":
      // The surrounding list draws the marker.
      return text("List item");
    case "quote":
      return <Blockquote>{text("Quote")}</Blockquote>;
    case "divider":
      return <Divider />;
    case "image":
    case "file":
      return block.attachmentId ? (
        <AttachmentBlock attachmentId={block.attachmentId} kind={block.kind} />
      ) : null;
    default:
      return text("Text");
  }
}
