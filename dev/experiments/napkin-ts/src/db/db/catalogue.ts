import * as X from "../../_extern/db-core.js";
import type { Db } from "../db.js";
import { dbError } from "../db.js";

async function withNode<T>(db: Db, f: (node: X.NodeState) => Promise<T> | T): Promise<T> {
  const node = await db.node.node.lock();
  try {
    return await f(node);
  } finally {
    db.node.node.unlock();
  }
}

const borrow = (db: Db): X.NodeState => {
  const n = db.node.node.tryLock();
  if (!n) throw new Error("synchronous node operation reentered a suspended operation");
  return n;
};

export const catalogueMethods = {
  authorSchemaLineagePublication(
    this: Db,
    schema: X.SchemaVersion,
    lens: X.MigrationLens,
    newTables: string[],
    droppedTables: string[],
  ) {
    return borrow(this).authorSchemaLineagePublication(schema, lens, newTables, droppedTables);
  },

  async publishSchema(this: Db, schema: X.SchemaVersion): Promise<X.SyncMessage[]> {
    this.checkCatalogueAdmin();
    const outcome = await withNode(this, (n) =>
      n.applyTrustedCatalogueMessage({
        kind: "PublishSchema",
        author: this.identity.author,
        schema,
      }),
    );
    return this.finishPublicationOutcome(outcome);
  },

  async publishSchemaWithLens(
    this: Db,
    catalogueSeq: number,
    publication: X.SchemaLineagePublication,
  ): Promise<X.SyncMessage[]> {
    this.checkCatalogueAdmin();
    const outcome = await withNode(this, (n) =>
      n.applyTrustedCatalogueMessage({
        kind: "PublishSchemaWithLens",
        author: this.identity.author,
        catalogueSeq,
        publication,
      }),
    );
    return this.finishPublicationOutcome(outcome);
  },

  async publishLens(this: Db, lens: X.MigrationLens): Promise<X.SyncMessage[]> {
    this.checkCatalogueAdmin();
    const outcome = await withNode(this, (n) =>
      n.applyTrustedCatalogueMessage({ kind: "PublishLens", author: this.identity.author, lens }),
    );
    return this.finishPublicationOutcome(outcome);
  },

  validateSchemaActivation(this: Db, revision: number, schema: X.JazzSchema): void {
    this.checkCatalogueAdmin();
    borrow(this).validateSchemaActivation(revision, schema);
  },

  async activateSchema(
    this: Db,
    revision: number,
    schema: X.JazzSchema,
    permissions: Map<string, X.TablePolicies>,
  ): Promise<void> {
    this.checkCatalogueAdmin();
    const source = structuredClone(schema.publicSchema()) as Map<string, any>;
    for (const [name, table] of source)
      table.policies = permissions.get(name) ?? X.TablePolicies.default();
    for (const name of permissions.keys())
      if (!source.has(name))
        throw X.nodeError(
          "InvalidCatalogueUpdate",
          "active schema permissions reference unknown table",
        );
    let compiled: X.JazzSchema;
    try {
      compiled = X.JazzSchemaT.new(source);
    } catch (error) {
      throw dbError("Schema", `invalid active schema permissions: ${error}`);
    }
    await withNode(this, (n) => n.activateSchema(revision, compiled));
    this.node.setPermissionsReady(true);
  },

  setPermissionsReady(this: Db, ready: boolean): void {
    this.node.setPermissionsReady(ready);
  },

  currentWriteSchema(this: Db): X.CurrentWriteSchema {
    return borrow(this).currentWriteSchema();
  },

  catalogueSchema(this: Db, schema: X.SchemaVersionId): X.JazzSchema | undefined {
    return borrow(this).catalogueSchemas().get(schema)?.schema;
  },

  activeCatalogueSeq(this: Db): number {
    return borrow(this).activeCatalogueSeq();
  },

  catalogueLens(this: Db, lens: X.MigrationLensId): X.MigrationLens | undefined {
    return borrow(this).catalogueLenses().get(lens);
  },
};
