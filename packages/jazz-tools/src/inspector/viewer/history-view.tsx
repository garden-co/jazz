import {
  AccountRole,
  BinaryStreamStart,
  CoID,
  JsonValue,
  LocalNode,
  OpID,
  RawCoValue,
  Role,
} from "cojson";
import { useMemo } from "react";
import { styled } from "goober";
import { isCoId } from "./types";
import { AccountOrGroupText } from "./account-or-group-text";
import { DataTable, ColumnDef } from "../ui/data-table";
import { MapOpPayload } from "cojson/dist/coValues/coMap.js";
import {
  DeletionOpPayload,
  InsertionOpPayload,
} from "cojson/dist/coValues/coList.js";
import {
  BinaryStreamChunk,
  BinaryStreamEnd,
} from "cojson/dist/coValues/coStream.js";
import { VerifiedTransaction } from "cojson/dist/coValueCore/coValueCore.js";
import { Icon, Accordion } from "../ui";

type HistoryEntry = {
  id: string;
  author: string;
  action: string;
  timestamp: Date;
  isValid: boolean;
  validationErrorMessage: string | undefined;
};

export function HistoryView({
  coValue,
  node,
}: {
  coValue: RawCoValue;
  node: LocalNode;
}) {
  const transactions = useMemo(
    () => getHistory(coValue),
    [coValue.core.verifiedTransactions.length],
  );

  const columns: ColumnDef<HistoryEntry>[] = [
    {
      id: "author",
      header: "Author",
      accessor: (row) => (
        <>
          {row.isValid || (
            <RedTooltip data-text="This transaction is invalid and is not used">
              <Icon
                name="caution"
                size="xs"
                color="red"
                style={{
                  display: "inline-block",
                  verticalAlign: "middle",
                  marginRight: "0.25rem",
                }}
              />
            </RedTooltip>
          )}
          {row.author.startsWith("co_") ? (
            <AccountOrGroupText
              coId={row.author as CoID<RawCoValue>}
              node={node}
              showId
            />
          ) : (
            row.author
          )}
        </>
      ),
      sortable: false,
      filterable: true,
      sortFn: (a, b) => a.author.localeCompare(b.author),
      filterFn: (row, filterValue) =>
        row.author.toLowerCase().includes(filterValue.toLowerCase()),
    },
    {
      id: "action",
      header: "Action",
      accessor: (row) => {
        if (row.isValid) return row.action;

        return (
          <>
            {row.action}
            <span style={{ color: "red", display: "block" }}>
              Invalid transaction: {row.validationErrorMessage}
            </span>
          </>
        );
      },
      sortable: false,
      filterable: true,
      sortFn: (a, b) => a.action.localeCompare(b.action),
    },
    {
      id: "timestamp",
      header: "Timestamp",
      accessor: (row) => row.timestamp.toISOString(),
      sortable: true,
      filterable: true,
      sortFn: (a, b) => a.timestamp.getTime() - b.timestamp.getTime(),
    },
  ];

  return (
    <Accordion title="CoValue history" storageKey="jazz-inspector-show-history">
      <DataTable
        columns={columns}
        data={transactions}
        pageSize={10}
        initialSort={{ columnId: "timestamp", direction: "desc" }}
        getRowKey={(row) => row.id}
        emptyMessage="No history available"
      />
    </Accordion>
  );
}

function getTransactionChanges(
  tx: VerifiedTransaction,
  coValue: RawCoValue,
): JsonValue[] {
  if (tx.isValid === false && tx.tx.privacy === "private") {
    const readKey = coValue.core.getReadKey(tx.tx.keyUsed);
    if (!readKey) {
      throw new Error("Read key not found");
    }

    return (
      coValue.core.verified.decryptTransaction(
        tx.txID.sessionID,
        tx.txID.txIndex,
        readKey,
      ) ?? []
    );
  }

  return tx.changes ?? (tx.tx as any).changes ?? [];
}

function getHistory(coValue: RawCoValue): HistoryEntry[] {
  return coValue.core.verifiedTransactions.flatMap((tx, index) => {
    const changes = getTransactionChanges(tx, coValue);

    return changes.map((change, changeIndex) => ({
      id: `${tx.txID.sessionID.toString()}-${tx.txID.txIndex}-${index}-${changeIndex}`,
      author: tx.author,
      action: mapTransactionToAction(change, coValue),
      timestamp: new Date(tx.currentMadeAt),
      isValid: tx.isValid,
      validationErrorMessage: tx.validationErrorMessage,
    }));
  });
}

function mapTransactionToAction(
  change: JsonValue,
  coValue: RawCoValue,
): string {
  // Group changes
  if (isUserPromotion(change)) {
    if (change.value === "revoked") {
      return `${change.key} has been revoked`;
    }

    return `${change.key} has been promoted to ${change.value}`;
  }

  if (isGroupExtension(change)) {
    const child = change.key.slice(6);
    return `Group became a member of ${child}`;
  }

  if (isGroupExtendRevocation(change)) {
    const child = change.key.slice(6);
    return `Group's membership of ${child} has been revoked.`;
  }

  if (isGroupPromotion(change)) {
    const parent = change.key.slice(7);
    return `Group ${parent} has been promoted to ${change.value}`;
  }

  if (isKeyRevelation(change)) {
    const [key, target] = change.key.split("_for_");
    return `Key "${key}" has been revealed to "${target}"`;
  }

  // coList changes
  if (isItemAppend(change)) {
    if (change.after === "start") {
      return `"${change.value}" has been appended`;
    }

    const after = findListChange(change.after, coValue);

    if (after === undefined) {
      return `"${change.value}" has been inserted after undefined item`;
    }

    return `"${change.value}" has been inserted after "${(after as any).value}"`;
  }

  if (isItemPrepend(change)) {
    if (change.before === "end") {
      return `"${change.value}" has been prepended`;
    }

    const before = findListChange(change.before, coValue);

    if (before === undefined) {
      return `"${change.value}" has been inserted before undefined item`;
    }

    return `"${change.value}" has been inserted before "${(before as any).value}"`;
  }

  if (isItemDeletion(change)) {
    const insertion = findListChange(change.insertion, coValue);
    if (insertion === undefined) {
      return `An undefined item has been deleted`;
    }

    return `"${(insertion as any).value}" has been deleted`;
  }

  // coStream changes
  if (isStreamStart(change)) {
    return `Stream started with mime type "${change.mimeType}" and file name "${change.fileName}"`;
  }

  if (isStreamChunk(change)) {
    return `Stream chunk added`;
  }

  if (isStreamEnd(change)) {
    return `Stream ended`;
  }

  // coMap changes
  if (isPropertySet(change)) {
    return `Property "${change.key}" has been set to ${JSON.stringify(change.value)}`;
  }

  if (isPropertyDeletion(change)) {
    return `Property "${change.key}" has been deleted`;
  }

  return "Unknown action: " + JSON.stringify(change);
}

const findListChange = (
  opId: OpID,
  coValue: RawCoValue,
): JsonValue | undefined => {
  return coValue.core.verifiedTransactions.find(
    (tx) =>
      tx.txID.sessionID === opId.sessionID && tx.txID.txIndex === opId.txIndex,
  )?.changes?.[opId.changeIdx];
};

const isGroupExtension = (
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "extend">,
  { op: "set" }
> => {
  return change?.op === "set" && change?.value === "extend";
};

const isGroupExtendRevocation = (
  change: any,
): change is Extract<
  MapOpPayload<`child_${string}`, "revoked">,
  { op: "set" }
> => {
  return change?.op === "set" && change?.value === "revoked";
};

const isGroupPromotion = (
  change: any,
): change is Extract<
  MapOpPayload<`parent_co_${string}`, AccountRole>,
  { op: "set" }
> => {
  return change?.op === "set" && change?.key.startsWith("parent_co_");
};

const isUserPromotion = (
  change: any,
): change is Extract<MapOpPayload<CoID<RawCoValue>, Role>, { op: "set" }> => {
  return (
    change?.op === "set" && (isCoId(change?.key) || change?.key === "everyone")
  );
};

const isKeyRevelation = (
  change: any,
): change is Extract<
  MapOpPayload<`${string}_for_${string}`, string>,
  { op: "set" }
> => {
  return change?.op === "set" && change?.key.includes("_for_");
};

const isPropertySet = (
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "set" }> => {
  return change?.op === "set" && "key" in change && "value" in change;
};
const isPropertyDeletion = (
  change: any,
): change is Extract<MapOpPayload<string, any>, { op: "del" }> => {
  return change?.op === "del" && "key" in change;
};

const isItemAppend = (
  change: any,
): change is Extract<InsertionOpPayload<any>, { op: "app" }> => {
  return change?.op === "app" && "after" in change && "value" in change;
};
const isItemPrepend = (
  change: any,
): change is Extract<InsertionOpPayload<any>, { op: "pre" }> => {
  return change?.op === "pre" && "before" in change && "value" in change;
};

const isItemDeletion = (
  change: any,
): change is Extract<DeletionOpPayload, { op: "del" }> => {
  return change?.op === "del" && "insertion" in change;
};

const isStreamStart = (change: any): change is BinaryStreamStart => {
  return change?.type === "start" && "mimeType" in change;
};

const isStreamChunk = (change: any): change is BinaryStreamChunk => {
  return change?.type === "chunk" && "chunk" in change;
};

const isStreamEnd = (change: any): change is BinaryStreamEnd => {
  return change?.type === "end";
};

const RedTooltip = styled("span")`
  position:relative; /* making the .tooltip span a container for the tooltip text */
  border-bottom:1px dashed #000; /* little indicater to indicate it's hoverable */

  &:before {
    content: attr(data-text);
    background-color: red;
    position:absolute;

    /* vertically center */
    top:50%;
    transform:translateY(-50%);

    /* move to right */
    left:100%;
    margin-left:15px; /* and add a small left margin */

    /* basic styles */
    width:200px;
    padding:10px;
    border-radius:10px;
    color: #fff;
    text-align:center;

    display:none; /* hide by default */
  }

  &:hover:before {
    display:block;
  }
`;
