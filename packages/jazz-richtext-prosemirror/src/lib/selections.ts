import { Node } from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { EditorState } from "prosemirror-state";

export enum SelectionMarkerType {
  Start = "start",
  End = "end",
}

type SelectionMarker = {
  sessionID: string;
  position: number;
  type: SelectionMarkerType;
};

type Selection = {
  sessionID: string;
  from: number;
  to: number;
};

type Selections = Record<string, Selection>;

const markerPrefix = "\\$sessionid";
const markerJoiner = "_";
const markerSuffix = "\\$";
const markerRegex = new RegExp(
  [
    markerPrefix,
    "(\\d+)",
    `(${Object.values(SelectionMarkerType).join("|")})${markerSuffix}`,
  ].join(markerJoiner),
  "g",
);

/**
 * Get all selection markers from a ProseMirror node
 * @param node - The ProseMirror node to get selection markers from
 * @param parentOffset - The offset of the parent node
 * @returns An array of selection markers
 */
export function getSelectionMarkersFromProseMirrorNode(
  node: Node,
  parentOffset = 0,
) {
  const markers: SelectionMarker[] = [];
  let text = node.textContent;

  if (node.type.name === "text") {
    let match;
    // Search for all selection markers in the text
    while ((match = markerRegex.exec(text)) !== null) {
      const [fullMatch, sessionID, type] = match;
      if (
        !sessionID ||
        (type !== SelectionMarkerType.Start && type !== SelectionMarkerType.End)
      )
        continue;

      // Add the selection marker to the list
      markers.push({
        sessionID,
        position: match.index + parentOffset,
        type,
      });

      // Remove the marker from the text
      text =
        text.slice(0, match.index) + text.slice(match.index + fullMatch.length);
      markerRegex.lastIndex = match.index;
    }
  }
  return markers;
}

/**
 * Combine markers into selections
 * @param markers - The markers to combine
 * @returns The combined selections
 */
export function combineMarkersIntoSelections(markers: SelectionMarker[]) {
  const selections: Selections = {};
  const pendingStarts: Record<string, SelectionMarker> = {};
  const pendingEnds: Record<string, SelectionMarker> = {};

  for (const marker of markers) {
    if (marker.type === "start") {
      // Check if we have a pending end for this session
      const pendingEnd = pendingEnds[marker.sessionID];
      if (pendingEnd) {
        selections[marker.sessionID] = {
          sessionID: marker.sessionID,
          from: marker.position,
          to: pendingEnd.position,
        };
        delete pendingEnds[marker.sessionID];
      } else {
        pendingStarts[marker.sessionID] = marker;
      }
    } else if (marker.type === "end") {
      // Check if we have a pending start for this session
      const pendingStart = pendingStarts[marker.sessionID];
      if (pendingStart) {
        selections[marker.sessionID] = {
          sessionID: marker.sessionID,
          from: pendingStart.position,
          to: marker.position,
        };
        delete pendingStarts[marker.sessionID];
      } else {
        pendingEnds[marker.sessionID] = marker;
      }
    }
  }

  return selections;
}

/**
 * Store the current selection into the ProseMirror document
 * @param state - The ProseMirror state
 * @returns The updated ProseMirror state
 */
export function storeCurrentSelectionIntoProseMirrorDocument(
  state: EditorState,
  sessionID: string,
) {
  const selection = state.selection;
  if (!selection) return state;

  // Create a transaction to update the document
  const tr = state.tr;

  // Insert the end marker
  const newEndNode = schema.text(
    `$sessionid_${sessionID}_${SelectionMarkerType.End}$`,
  );
  tr.replaceWith(selection.to, selection.to, newEndNode);

  // Insert the start marker
  const newStartNode = schema.text(
    `$sessionid_${sessionID}_${SelectionMarkerType.Start}$`,
  );
  tr.replaceWith(selection.from, selection.from, newStartNode);

  return state.apply(tr);
}
