import { useEffect, useState } from "react";
import { SchemaDiagramCanvas } from "./SchemaCanvas.js";
import { compareSchemas, hasCompatibilityPath, shortHash } from "./schema-analysis.js";
import { buildComparisonDiagram } from "./schema-diagram-model.js";
import { useSchemaExplorerContext } from "./index.js";
import styles from "./view.module.css";

export function SchemaComparisonView() {
  const catalog = useSchemaExplorerContext();
  const [leftHash, setLeftHash] = useState<string>("");
  const [rightHash, setRightHash] = useState<string>("");

  useEffect(() => {
    if (!catalog.supportsCatalogue) {
      return;
    }

    const nextLeft = catalog.currentSchemaHash ?? catalog.hashes[0] ?? "";
    const nextRight = catalog.hashes.find((hash) => hash !== nextLeft) ?? catalog.hashes[0] ?? "";

    setLeftHash(nextLeft);
    setRightHash(nextRight);
  }, [catalog.currentSchemaHash, catalog.hashes, catalog.supportsCatalogue]);

  if (!catalog.supportsCatalogue) {
    return (
      <section className={styles.view}>
        <div className={styles.notice}>
          Schema comparison needs standalone mode because it reads multiple stored schemas from the
          server catalogue.
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

  if (catalog.hashes.length < 2 || !leftHash || !rightHash) {
    return (
      <section className={styles.view}>
        <div className={styles.notice}>At least two stored schemas are needed for comparison.</div>
      </section>
    );
  }

  const leftSchema = catalog.schemas[leftHash];
  const rightSchema = catalog.schemas[rightHash];

  if (!leftSchema || !rightSchema) {
    return (
      <section className={styles.view}>
        <div className={styles.notice}>One of the selected schemas is not available yet.</div>
      </section>
    );
  }

  const compatibilityPathExists = hasCompatibilityPath(leftHash, rightHash, catalog.migrations);
  const comparison = compareSchemas(leftSchema, rightSchema, {
    hasCompatibilityPath: compatibilityPathExists,
  });
  const diagram = buildComparisonDiagram(comparison);

  return (
    <section className={styles.view}>
      <div className={styles.toolbar}>
        <label className={styles.field}>
          Left schema
          <select value={leftHash} onChange={(event) => setLeftHash(event.target.value)}>
            {catalog.hashes.map((hash) => (
              <option key={hash} value={hash}>
                {shortHash(hash)}
              </option>
            ))}
          </select>
        </label>
        <label className={styles.field}>
          Right schema
          <select value={rightHash} onChange={(event) => setRightHash(event.target.value)}>
            {catalog.hashes.map((hash) => (
              <option key={hash} value={hash}>
                {shortHash(hash)}
              </option>
            ))}
          </select>
        </label>
        <div
          className={`${styles.pathBadge} ${compatibilityPathExists ? styles.pathBadgeOk : styles.pathBadgeMissing}`}
        >
          {compatibilityPathExists ? "Migration path exists" : "No migration path"}
        </div>
      </div>
      <div className={styles.copyBlock}>
        <h3 className={styles.heading}>Schema diff</h3>
        <p className={styles.copy}>
          Added and removed tables or columns are color-coded. When the selected pair has no
          compatibility path, same-shape unmatched columns are called out as unknown mappings.
        </p>
      </div>
      <SchemaDiagramCanvas
        nodes={diagram.nodes}
        edges={diagram.edges}
        emptyState="No changed tables or relationships were found between the selected schemas."
      />
    </section>
  );
}
