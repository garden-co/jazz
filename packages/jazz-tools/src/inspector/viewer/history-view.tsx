import { CoID, JsonValue, LocalNode, OpID, RawCoValue } from "cojson";
import { useMemo } from "react";
import { styled } from "goober";
import { AccountOrGroupText } from "./account-or-group-text";
import { DataTable, ColumnDef } from "../ui/data-table";
import { Icon, Accordion } from "../ui";
import * as TransactionChanges from "../utils/transactions-changes";
import { getTransactionChanges } from "../utils/history";

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
  if (TransactionChanges.isUserPromotion(change)) {
    if (change.value === "revoked") {
      return `${change.key} has been revoked`;
    }

    return `${change.key} has been promoted to ${change.value}`;
  }

  if (TransactionChanges.isGroupExtension(change)) {
    const child = change.key.slice(6);
    return `Group became a member of ${child}`;
  }

  if (TransactionChanges.isGroupExtendRevocation(change)) {
    const child = change.key.slice(6);
    return `Group's membership of ${child} has been revoked.`;
  }

  if (TransactionChanges.isGroupPromotion(change)) {
    const parent = change.key.slice(7);
    return `Group ${parent} has been promoted to ${change.value}`;
  }

  if (TransactionChanges.isKeyRevelation(change)) {
    const [key, target] = change.key.split("_for_");
    return `Key "${key}" has been revealed to "${target}"`;
  }

  // coList changes
  if (TransactionChanges.isItemAppend(change)) {
    if (change.after === "start") {
      return `"${change.value}" has been appended`;
    }

    const after = findListChange(change.after, coValue);

    if (after === undefined) {
      return `"${change.value}" has been inserted after undefined item`;
    }

    return `"${change.value}" has been inserted after "${(after as any).value}"`;
  }

  if (TransactionChanges.isItemPrepend(change)) {
    if (change.before === "end") {
      return `"${change.value}" has been prepended`;
    }

    const before = findListChange(change.before, coValue);

    if (before === undefined) {
      return `"${change.value}" has been inserted before undefined item`;
    }

    return `"${change.value}" has been inserted before "${(before as any).value}"`;
  }

  if (TransactionChanges.isItemDeletion(change)) {
    const insertion = findListChange(change.insertion, coValue);
    if (insertion === undefined) {
      return `An undefined item has been deleted`;
    }

    return `"${(insertion as any).value}" has been deleted`;
  }

  // coStream changes
  if (TransactionChanges.isStreamStart(change)) {
    return `Stream started with mime type "${change.mimeType}" and file name "${change.fileName}"`;
  }

  if (TransactionChanges.isStreamChunk(change)) {
    return `Stream chunk added`;
  }

  if (TransactionChanges.isStreamEnd(change)) {
    return `Stream ended`;
  }

  // coMap changes
  if (TransactionChanges.isPropertySet(change)) {
    return `Property "${change.key}" has been set to ${JSON.stringify(change.value)}`;
  }

  if (TransactionChanges.isPropertyDeletion(change)) {
    return `Property "${change.key}" has been deleted`;
  }

  if ((change as any).op === "custom") {
    return (change as any).action;
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
