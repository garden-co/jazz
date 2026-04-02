import { SchemaCompatibilityGraph } from "./SchemaCanvas.js";
import { findShortestGhostEdges, getSchemaStats, shortHash } from "./schema-analysis.js";
import { useSchemaExplorerContext } from "./index.js";
import styles from "./view.module.css";

export function SchemaCompatibilityView() {
  const catalog = useSchemaExplorerContext();

  if (!catalog.supportsCatalogue) {
    return (
      <section className={styles.view}>
        <div className={styles.notice}>
          Full compatibility graphs need standalone mode because the DevTools panel only knows the
          currently registered runtime schema.
        </div>
      </section>
    );
  }

  if (catalog.loading) {
    return (
      <section className={styles.view}>
        <div className={styles.notice}>Loading schema catalogue…</div>
      </section>
    );
  }

  const ghostEdges = findShortestGhostEdges({
    schemas: catalog.schemas,
    migrations: catalog.migrations,
  });
  const nodes = catalog.hashes.map((hash) => ({
    id: hash,
    hash,
    stats: getSchemaStats(catalog.schemas[hash] ?? catalog.currentSchema),
    selected: hash === catalog.currentSchemaHash,
  }));
  const edges = [
    ...catalog.migrations.map((migration) => ({
      id: `migration:${migration.fromHash}:${migration.toHash}`,
      source: migration.fromHash,
      target: migration.toHash,
      kind: "migration" as const,
      label: "migration",
    })),
    ...ghostEdges.map((edge) => ({
      id: `ghost:${edge.fromHash}:${edge.toHash}`,
      source: edge.fromHash,
      target: edge.toHash,
      kind: "ghost" as const,
      label: "missing edge",
    })),
  ];

  return (
    <section className={styles.view}>
      <div className={styles.copyBlock}>
        <h3 className={styles.heading}>Compatibility graph</h3>
        <p className={styles.copy}>
          Solid edges are published migrations. Dashed ghost edges connect disconnected schema
          groups using the smallest structural distance we can infer from the stored schemas.
        </p>
      </div>
      <SchemaCompatibilityGraph nodes={nodes} edges={edges} />
      {ghostEdges.length > 0 ? (
        <div className={styles.inlineList}>
          {ghostEdges.map((edge) => (
            <div key={`${edge.fromHash}:${edge.toHash}`} className={styles.inlineListItem}>
              Suggested bridge: {shortHash(edge.fromHash)} {"->"} {shortHash(edge.toHash)}
            </div>
          ))}
        </div>
      ) : null}
    </section>
  );
}
