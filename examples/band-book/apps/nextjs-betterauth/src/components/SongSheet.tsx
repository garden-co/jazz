"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import type { TextRange } from "jazz-tools";
import {
  applyMarkdown,
  captureRange,
  markdownSync,
  overlays,
  type Overlay,
  type PlacedOverlay,
  presenceOverlays,
  setOverlays,
} from "jazz-tools/prosemirror";
import { EditorState, TextSelection } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import "prosemirror-view/style/prosemirror.css";
import {
  Avatar,
  AvatarGroup,
  Badge,
  Button,
  Card,
  HStack,
  Skeleton,
  Text,
  TextArea,
  ToggleButton,
  Toolbar,
  VStack,
} from "@astryxdesign/core";
import { app, type SheetComment, type SongSheet as SongSheetRow } from "@/schema";
import { editingPlugins, parseSheet, SHEET_TOOLS, toggleLink } from "@/src/lib/song-editor";
import { useSheetPresence } from "./use-sheet-presence";
import { memberName, useWorkspace } from "./workspace-context";

/** How often typing writes the sheet: the first edit at once, then at most this often. */
const WRITE_EVERY_MS = 300;

/**
 * A song sheet: one rich-text document stored as Markdown, with everyone
 * else's cursors and a comment sidebar drawn over it.
 */
export function SongSheet({ pageId, editable }: { pageId: string; editable: boolean }) {
  const { data: sheets } = useAll(app.songSheets.where({ pageId }));
  const sheet = sheets?.[0];
  if (!sheet) return <Skeleton height={240} />;
  return <SheetEditor key={sheet.id} sheet={sheet} editable={editable} />;
}

function SheetEditor({ sheet, editable }: { sheet: SongSheetRow; editable: boolean }) {
  const db = useDb();
  const { me, members, workspace } = useWorkspace();
  const mount = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<EditorView | null>(null);
  const [state, setState] = useState<EditorState | null>(null);
  const [placed, setPlaced] = useState<readonly PlacedOverlay[]>([]);
  const presence = useSheetPresence(sheet.pageId);
  const report = useRef(presence.report);
  report.current = presence.report;

  // #region rich-text-editor
  useEffect(() => {
    const editor = new EditorView(mount.current!, {
      state: EditorState.create({
        doc: parseSheet(sheet.body),
        plugins: [
          ...editingPlugins(),
          // Whole-value writes: the first edit goes out at once, then at most
          // one write every 300 ms while typing. Each write is one version,
          // and the authority merges it with whatever others wrote meanwhile.
          markdownSync({
            parse: parseSheet,
            writeEvery: WRITE_EVERY_MS,
            onChange: (body) => db.update(app.songSheets, sheet.id, { body }),
          }),
          overlays({ anchors: () => db.textAnchors(), onPlaced: setPlaced }),
        ],
      }),
      editable: () => editable,
      dispatchTransaction(tr) {
        const next = editor.state.apply(tr);
        editor.updateState(next);
        setState(next);
        if (tr.selectionSet || tr.docChanged) report.current(next);
      },
    });
    setView(editor);
    setState(editor.state);
    return () => editor.destroy();
  }, [db, sheet.id]);

  // A newer value (someone else's edit, merged by the authority) is applied
  // as the smallest change, so the cursor stays on the text it was on, and
  // edits typed since the last write are kept on top of it.
  useEffect(() => {
    if (!view) return;
    const tr = applyMarkdown(view.state, sheet.body);
    if (tr) view.dispatch(tr);
  }, [view, sheet.body]);
  // #endregion rich-text-editor

  // #region rich-text-overlays
  const { data: comments } = useAll(
    app.sheetComments.where({ pageId: sheet.pageId, resolved: false }),
  );
  const drawn = useMemo<Overlay[]>(
    () => [
      ...(comments ?? []).map((comment) => ({
        id: comment.id,
        range: comment.range as unknown as TextRange,
        attrs: { class: "bb-sheet-comment" },
      })),
      ...presenceOverlays(presence.collaborators),
    ],
    [comments, presence.collaborators],
  );
  useEffect(() => {
    if (view) view.dispatch(setOverlays(view.state.tr, drawn));
  }, [view, drawn]);
  // #endregion rich-text-overlays

  // #region rich-text-reanchor
  // A comment found with less confidence than usual drifted: its words were
  // edited. Its author stores it again from the current text, so later
  // readers find it at full confidence.
  useEffect(() => {
    if (!view) return;
    for (const spot of placed) {
      const comment = comments?.find((row) => row.id === spot.overlay.id);
      if (!comment || !spot.drifted || comment.author !== me) continue;
      db.update(app.sheetComments, comment.id, {
        range: captureRange(view.state, db.textAnchors(), spot.from, spot.to),
      });
    }
  }, [placed]);
  // #endregion rich-text-reanchor

  const selection = state?.selection;
  const run = (command: (typeof SHEET_TOOLS)[number][number]["run"]) => {
    if (!view) return;
    command(view.state, view.dispatch, view);
    view.focus();
  };

  return (
    <div className="bb-sheet-layout">
      <div className="bb-sheet-main">
        <VStack gap={3} width="100%">
          <HStack gap={2} justify="between" align="center" wrap="wrap">
            {editable && state && (
              <Toolbar
                label="Formatting"
                size="sm"
                startContent={
                  <HStack gap={1} wrap="wrap">
                    {SHEET_TOOLS.flatMap((group, index) => [
                      ...group.map((tool) => (
                        <ToggleButton
                          key={tool.label}
                          label={tool.label}
                          tooltip={tool.label}
                          size="sm"
                          isPressed={tool.isActive?.(state) ?? false}
                          isDisabled={!tool.run(state)}
                          onPressedChange={() => run(tool.run)}
                        >
                          {tool.short}
                        </ToggleButton>
                      )),
                      index < SHEET_TOOLS.length - 1 ? <span key={`gap${index}`} /> : null,
                    ])}
                    <ToggleButton
                      label="Link"
                      tooltip="Link"
                      size="sm"
                      isDisabled={!selection || selection.empty}
                      onPressedChange={() => {
                        const href = window.prompt("Link to");
                        if (href !== null) run(toggleLink(href || null));
                      }}
                    >
                      🔗
                    </ToggleButton>
                  </HStack>
                }
              />
            )}
            {presence.collaborators.length > 0 && (
              <AvatarGroup size="sm">
                {presence.collaborators.map((person) => (
                  <Avatar key={person.id} name={person.name} tooltip={`${person.name} is here`} />
                ))}
              </AvatarGroup>
            )}
          </HStack>
          <div ref={mount} className="bb-sheet" aria-label="Song sheet" />
        </VStack>
      </div>
      <CommentSidebar
        view={view}
        state={state}
        comments={comments ?? []}
        placed={placed}
        workspaceId={workspace.id}
        pageId={sheet.pageId}
        name={(account) => memberName(members, account)}
      />
    </div>
  );
}

// #region rich-text-comments
function CommentSidebar(props: {
  view: EditorView | null;
  state: EditorState | null;
  comments: SheetComment[];
  placed: readonly PlacedOverlay[];
  workspaceId: string;
  pageId: string;
  name: (account: string) => string;
}) {
  const db = useDb();
  const { me } = useWorkspace();
  const [draft, setDraft] = useState("");
  const { view, state, placed } = props;
  const selection = state?.selection;

  const addComment = () => {
    if (!view || !selection || selection.empty || !draft.trim()) return;
    db.insert(app.sheetComments, {
      workspaceId: props.workspaceId,
      pageId: props.pageId,
      author: me,
      // The commented text and the text around it: anyone finds it again,
      // however the sheet changes after this.
      range: captureRange(view.state, db.textAnchors(), selection.from, selection.to),
      text: draft.trim(),
      resolved: false,
    });
    setDraft("");
  };

  const spotOf = (id: string) => placed.find((spot) => spot.overlay.id === id);
  const ordered = [...props.comments].sort(
    (a, b) => (spotOf(a.id)?.from ?? Infinity) - (spotOf(b.id)?.from ?? Infinity),
  );

  return (
    <div className="bb-sheet-comments">
      <VStack gap={3} width="100%">
        <Text type="label">Comments</Text>
        <VStack gap={2}>
          <TextArea
            label="New comment"
            isLabelHidden
            rows={2}
            value={draft}
            placeholder={
              selection && !selection.empty ? "Comment on the selection" : "Select text to comment"
            }
            isDisabled={!selection || selection.empty}
            onChange={setDraft}
          />
          <Button
            label="Comment"
            size="sm"
            isDisabled={!selection || selection.empty || !draft.trim()}
            onClick={addComment}
          />
        </VStack>
        {ordered.map((comment) => {
          const spot = spotOf(comment.id);
          // The commented words as they read now, or as they were stored.
          const quote =
            spot && !spot.orphaned && state
              ? state.doc.textBetween(spot.from, spot.to, " ")
              : (comment.range as unknown as TextRange).matched;
          return (
            <Card key={comment.id} padding={3}>
              <VStack gap={1}>
                <HStack gap={2} justify="between" align="center">
                  <Text type="supporting">{props.name(comment.author)}</Text>
                  {spot?.orphaned && <Badge label="Text removed" />}
                </HStack>
                <Text type="supporting">“{quote}”</Text>
                <Text>{comment.text}</Text>
                <HStack gap={2}>
                  {spot && !spot.orphaned && view && (
                    <Button
                      label="Show"
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        view.dispatch(
                          view.state.tr
                            .setSelection(TextSelection.create(view.state.doc, spot.from, spot.to))
                            .scrollIntoView(),
                        );
                        view.focus();
                      }}
                    />
                  )}
                  <Button
                    label="Resolve"
                    size="sm"
                    variant="ghost"
                    onClick={() => db.update(app.sheetComments, comment.id, { resolved: true })}
                  />
                </HStack>
              </VStack>
            </Card>
          );
        })}
      </VStack>
    </div>
  );
}
// #endregion rich-text-comments
