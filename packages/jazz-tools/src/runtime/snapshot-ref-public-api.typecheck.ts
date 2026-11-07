import {
  type BranchBase,
  type Db,
  type QueryOptions,
  type SnapshotRef,
  parseSnapshotRef,
  serializeSnapshotRef,
} from "../index.js";

declare const db: Db;

async function frozenBaseContract() {
  const snapshot: SnapshotRef = await db.captureSnapshot();
  const restored: SnapshotRef = parseSnapshotRef(serializeSnapshotRef(snapshot));
  const options: QueryOptions = { branch: "draft", base: ["main", restored] };
  // @ts-expect-error Persisted strings must be restored with the public parser.
  const unparsed: BranchBase = ["main", "stored reference"];
  // @ts-expect-error Arbitrary objects are not public snapshot references.
  const fabricated: BranchBase = ["main", { owner: "node", global_base: 1 }];
  void [options, unparsed, fabricated];
}
void frozenBaseContract;
