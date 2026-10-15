// Core-owned current-row evaluation. Nothing here turns cache completeness into authority.
import { SortedSet } from "../../support/collections.js";
import {
  cmpTxId,
  expandVersionCarriers,
  nodeError,
  cmpGlobalTime,
} from "../../_extern/node-policy-peer.js";

export const canMintCurrentRowReceipts = (node: any): boolean =>
  node.authoritativeScalarExitRefresh && node.clientRelayScope() === undefined;

export function currentRowCoordinate(node: any, table: string, row: string): any {
  const schema = node.catalogue.localSchemaVersionId;
  const physical = node.catalogue.physicalMappings.get(schema)?.identities.tables.get(table);
  if (!physical) throw nodeError("TableNotFound", table);
  return { schema, table, physicalTable: physical.id, row };
}

const coordinateEquals = (a: any, b: any): boolean =>
  !!a &&
  a.schema === b.schema &&
  a.table === b.table &&
  a.physicalTable === b.physicalTable &&
  a.row === b.row;

function tryCoordinate(node: any, table: string, row: string): any {
  try {
    return currentRowCoordinate(node, table, row);
  } catch {
    return undefined;
  }
}

/** Caller holds the node owner lock throughout evaluation and receipt capture. */
export async function evaluateCurrentRows(
  node: any,
  request: any,
  context: any,
  coreEpoch: number,
  progress: number,
): Promise<any> {
  const receipt = {
    requestId: request.requestId,
    rows: request.rows.slice(),
    outcomes: request.rows.map(() => "Unknown" as string),
    context,
    core: node.nodeUuid(),
    coreEpoch,
    claimsRevision: node.sessionClaimRevision(context.identity),
    policyEpoch: node.activeCatalogueSeq(),
    settledThrough: node.committedGlobalTime(),
    authorizationProgress: progress,
    versionCarriers: [] as any[],
  };
  if (!canMintCurrentRowReceipts(node) || !node.permissionsReady()) return receipt;
  for (let index = 0; index < request.rows.length; index++) {
    const coordinate = request.rows[index];
    if (!coordinateEquals(tryCoordinate(node, coordinate.table, coordinate.row), coordinate))
      continue;
    if (node.table(coordinate.table).branchBy.length) continue;
    const [shape, binding] = node.wholeTableShapeBinding(coordinate.table);
    const rows = await node.queryReadableCurrentRowIncludingDeleted(
      shape,
      binding,
      "Global",
      context.identity,
      coordinate.row,
    );
    if (!rows.some((r: any) => r.rowUuid() === coordinate.row)) {
      receipt.outcomes[index] = "CurrentUnavailable";
      continue;
    }
    const tableId = node.physicalTableIdForSchema(coordinate.schema, coordinate.table);
    const txId = await node.visibleGlobalTxIdForPhysicalTableNow(tableId, coordinate.row);
    if (txId === undefined) continue;
    let carriers: any[] = [];
    for (const tx of new SortedSet(cmpTxId, [txId])) {
      const versions = (await node.queryVersionsForTx(tx)).filter((v: any) => {
        if (v.rowUuid() !== coordinate.row) return false;
        try {
          return node.physicalTableIdForVersion(v) === tableId;
        } catch {
          return false;
        }
      });
      if (versions.length === 0 || versions.some((v: any) => v.branchKey().values.length > 0)) {
        carriers = [];
        break;
      }
      const stored = await node.queryTransaction(tx);
      if (!stored) throw nodeError("MissingTransaction", undefined, tx);
      const bundle = await node.versionBundleForMaintainedViewVersionsWithTx(stored, versions);
      bundle.scope = "ViewScoped";
      carriers.push({ kind: "Bundle", bundle });
    }
    if (carriers.length) {
      receipt.outcomes[index] = "Readable";
      receipt.versionCarriers.push(...carriers);
    }
  }
  return receipt;
}

/** Validate the whole receipt against exact coordinates before repair ingestion. */
export async function ingestCurrentRowsReceipt(node: any, receipt: any): Promise<void> {
  const invalid = () =>
    nodeError("MalformedViewUpdate", "current row receipt does not match requested coordinates");
  if (receipt.rows.length !== receipt.outcomes.length) throw invalid();
  let bundles: any[];
  try {
    bundles = expandVersionCarriers(receipt.versionCarriers);
  } catch {
    throw invalid();
  }
  const covered = new Set<number>();
  const requests: any[] = [];
  for (const bundle of bundles) {
    if (
      bundle.scope !== "ViewScoped" ||
      bundle.versions.length === 0 ||
      bundle.globalTime === undefined ||
      cmpGlobalTime(bundle.globalTime, receipt.settledThrough) > 0
    )
      throw invalid();
    for (const version of bundle.versions) {
      const physical = node.catalogue.physicalMappings
        .get(version.schemaVersion())
        ?.identities.tables.get(version.table())?.id;
      if (physical === undefined) throw invalid();
      const index = receipt.rows.findIndex(
        (r: any) => r.physicalTable === physical && r.row === version.rowUuid(),
      );
      if (index < 0) throw invalid();
      const coordinate = receipt.rows[index];
      if (
        receipt.outcomes[index] !== "Readable" ||
        version.branchKey().values.length > 0 ||
        !coordinateEquals(tryCoordinate(node, coordinate.table, coordinate.row), coordinate)
      )
        throw invalid();
      covered.add(index);
      requests.push({ table: coordinate.table, row: coordinate.row, txId: bundle.tx.txId });
    }
  }
  if (receipt.outcomes.some((o: string, i: number) => o === "Readable" && !covered.has(i)))
    throw invalid();
  await node.applyRowVersionPayloadsForRequests(requests, bundles);
}
