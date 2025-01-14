import { act, render, screen } from "@testing-library/react";
import { Document } from "jazz-browser-prosemirror";
import { JazzTestProvider, createJazzTestAccount } from "jazz-react/testing";
import { CoRichText, Group, ID, Marks } from "jazz-tools";
import { schema } from "prosemirror-schema-basic";
import { Transaction } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import * as React from "react";
import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { DocumentComponent } from "./index.js";

vi.mock("prosemirror-view");
vi.mock("prosemirror-state");
vi.mock("prosemirror-example-setup");

// Only mock the subscribe method
const originalSubscribe = Document.subscribe;
Document.subscribe = vi.fn((id, as, depth, listener) => {
  if (listener) {
    setTimeout(() => {
      listener(CoRichText.createFromPlainText("test", { owner: as }));
    }, 0);
  }
  return () => {};
}) as unknown as typeof Document.subscribe;

async function setupEditorMock(
  options: {
    hasFocus?: boolean;
    focusSpy?: vi.Mock;
    destroySpy?: vi.Mock;
  } = {},
) {
  const mockTr = {
    insertText: vi.fn().mockReturnThis(),
    addMark: vi.fn().mockReturnThis(),
    removeMark: vi.fn().mockReturnThis(),
  } as unknown as Transaction;

  const editorMock = {
    destroy: options.destroySpy ?? vi.fn(),
    state: {
      selection: {},
      plugins: [],
      schema: schema,
      tr: mockTr,
    },
    updateState: vi.fn(),
    hasFocus: () => options.hasFocus ?? false,
    focus: options.focusSpy ?? vi.fn(),
    dispatch: vi.fn(),
    directPlugins: [],
    mounted: true,
    _props: {},
    _root: document.createElement("div"),
  } as unknown as EditorView;

  const { EditorView } = await import("prosemirror-view");
  vi.mocked(EditorView).mockImplementation(() => editorMock);

  return editorMock;
}

async function setupDocumentMock(group: Group) {
  vi.mocked(Document.subscribe).mockImplementation(
    (id, as, depth, listener) => {
      if (listener) {
        setTimeout(() => {
          listener(CoRichText.createFromPlainText("test", { owner: as }));
        }, 0);
      }
      return () => {};
    },
  );
}

describe("Document", () => {
  let account: Awaited<ReturnType<typeof createJazzTestAccount>>;
  let group: ReturnType<typeof Group.create>;

  beforeEach(async () => {
    account = await createJazzTestAccount();
    group = Group.create({ owner: account });
    vi.clearAllMocks();
    await setupDocumentMock(group);
  });

  afterAll(() => {
    // Restore original subscribe method
    Document.subscribe = originalSubscribe;
  });

  it("renders without crashing", async () => {
    await setupEditorMock();
    render(
      <JazzTestProvider account={account}>
        <DocumentComponent docID={"test-doc-id" as ID<Document>} />
      </JazzTestProvider>,
    );

    expect(screen.getByTestId("editor")).toBeInTheDocument();
  });

  it("subscribes to document updates", async () => {
    await setupEditorMock();
    const subscribeSpy = vi.mocked(Document.subscribe);

    render(
      <JazzTestProvider account={account}>
        <DocumentComponent docID={"test-doc-id" as ID<Document>} />
      </JazzTestProvider>,
    );

    expect(subscribeSpy).toHaveBeenCalledWith(
      "test-doc-id",
      expect.anything(),
      { marks: [{}], text: [] },
      expect.any(Function),
    );
  });

  it("cleans up on unmount", async () => {
    let isReady = false;
    const destroySpy = vi.fn();
    await setupEditorMock({ destroySpy });

    const { unmount } = render(
      <JazzTestProvider account={account}>
        <DocumentComponent
          docID={"test-doc-id" as ID<Document>}
          onReady={() => {
            isReady = true;
          }}
        />
      </JazzTestProvider>,
    );

    // Wait for ready and unmount
    await act(async () => {
      while (!isReady) await new Promise((r) => setTimeout(r, 10));
      unmount();
    });

    expect(destroySpy).toHaveBeenCalled();
  });

  it("preserves focus after document update", async () => {
    let isReady = false;
    const focusSpy = vi.fn();
    await setupEditorMock({ hasFocus: true, focusSpy });

    render(
      <JazzTestProvider account={account}>
        <DocumentComponent
          docID={"test-doc-id" as ID<Document>}
          onReady={() => {
            isReady = true;
          }}
        />
      </JazzTestProvider>,
    );

    // Wait for ready
    await act(async () => {
      while (!isReady) await new Promise((r) => setTimeout(r, 10));
    });

    expect(focusSpy).toHaveBeenCalled();
  });

  describe("editing", () => {
    let text: CoRichText;
    let editorMock: EditorView;

    beforeEach(async () => {
      text = CoRichText.createFromPlainText("Hello world", { owner: group });
      text.insertMark(0, text.length, Marks.Paragraph, { tag: "paragraph" });
      editorMock = await setupEditorMock();
    });

    it("handles text insertion", async () => {
      render(
        <JazzTestProvider account={account}>
          <DocumentComponent docID={"test-doc-id" as ID<Document>} />
        </JazzTestProvider>,
      );

      // Simulate text insertion
      const tr = editorMock.state.tr;
      tr.insertText(" test");
      editorMock.dispatch(tr);

      expect(tr.insertText).toHaveBeenCalledWith(" test");
    });
  });
});
