import { schema } from "prosemirror-schema-basic";
import { EditorState, TextSelection } from "prosemirror-state";
import { describe, expect, it } from "vitest";
import {
  SelectionMarkerType,
  combineMarkersIntoSelections,
  getSelectionMarkersFromProseMirrorNode,
  storeCurrentSelectionIntoProseMirrorDocument,
} from "../lib/selections";

describe("selections", () => {
  it("should be able to get selections from html", () => {
    const node = schema.text(
      "Hello $sessionid_123_start$$sessionid_123_end$world",
    );

    const selections = getSelectionMarkersFromProseMirrorNode(node);

    expect(selections).toEqual([
      {
        position: 6,
        sessionID: "123",
        type: "start",
      },
      {
        position: 6,
        sessionID: "123",
        type: "end",
      },
    ]);
  });

  it("should combine markers into selections", () => {
    const markers = [
      { sessionID: "123", position: 5, type: SelectionMarkerType.Start },
      { sessionID: "123", position: 10, type: SelectionMarkerType.End },
      { sessionID: "456", position: 15, type: SelectionMarkerType.End },
      { sessionID: "456", position: 20, type: SelectionMarkerType.Start },
    ];

    const selections = combineMarkersIntoSelections(markers);

    expect(selections).toEqual({
      "123": {
        sessionID: "123",
        from: 5,
        to: 10,
      },
      "456": {
        sessionID: "456",
        from: 20,
        to: 15,
      },
    });
  });

  it("should store the current selection into the document", () => {
    // Create a new editor state with some content
    const state = EditorState.create({
      schema,
      doc: schema.node("doc", null, [
        schema.node("paragraph", null, [schema.text("Hello world")]),
      ]),
    });
    const sessionID = "123";

    // Set a selection in the document
    const tr = state.tr;
    tr.setSelection(TextSelection.create(state.doc, 7, 7)); // ProseMirror positioning
    const stateWithSelection = state.apply(tr);

    // Verify the selection was created
    expect(stateWithSelection.selection.from).toEqual(
      stateWithSelection.selection.to,
    );

    // Call the function to store the selection
    const updatedState = storeCurrentSelectionIntoProseMirrorDocument(
      stateWithSelection,
      sessionID,
    );

    // Verify the selection was stored correctly
    expect(updatedState.doc.textContent).toBe(
      "Hello $sessionid_123_start$$sessionid_123_end$world",
    );
  });
});
