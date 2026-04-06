import { SchemaDiagramCanvas } from "./SchemaCanvas.js";
import { buildSingleSchemaDiagram } from "./schema-diagram-model.js";
import { shortHash } from "./schema-analysis.js";
import { useSchemaExplorerContext } from "./index.js";
import styles from "./view.module.css";

export function SingleSchemaView() {
  const catalog = useSchemaExplorerContext();
  const diagram = buildSingleSchemaDiagram(
    catalog.currentSchema,
    catalog.currentSchemaHash ? shortHash(catalog.currentSchemaHash) : "runtime schema",
  );

  return (
    <section className={styles.view}>
      <div className={styles.copyBlock}>
        <h3 className={styles.heading}>Current schema</h3>
        <p className={styles.copy}>
          Tables are rendered as cards, and reference columns emit edges toward the referenced
          table.
        </p>
      </div>
      <SchemaDiagramCanvas
        nodes={diagram.nodes}
        edges={diagram.edges}
        emptyState="The current schema does not define any tables."
      />
    </section>
  );
}
