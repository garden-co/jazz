import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import ELK from "elkjs/lib/elk.bundled.js";
import { useEffect, useState } from "react";
import type { SchemaDiffState, SchemaStats, UnknownColumnMapping } from "./schema-analysis.js";
import { shortHash } from "./schema-analysis.js";
import styles from "./SchemaCanvas.module.css";

const elk = new ELK();
const TABLE_NODE_WIDTH = 340;
const HASH_NODE_WIDTH = 220;

export interface SchemaTableColumnView {
  name: string;
  typeLabel: string;
  nullable: boolean;
  reference?: string;
  state: SchemaDiffState;
}

export interface SchemaTableNodeView extends Record<string, unknown> {
  id: string;
  tableName: string;
  state: Exclude<SchemaDiffState, "unknown">;
  subtitle?: string;
  columns: SchemaTableColumnView[];
  unknownMappings?: UnknownColumnMapping[];
}

export interface SchemaTableReferenceView {
  id: string;
  sourceTable: string;
  sourceColumn: string;
  targetTable: string;
  label?: string;
  state: Exclude<SchemaDiffState, "unknown">;
}

export interface SchemaVersionNodeView extends Record<string, unknown> {
  id: string;
  hash: string;
  stats: SchemaStats;
  selected?: boolean;
}

export interface SchemaVersionEdgeView {
  id: string;
  source: string;
  target: string;
  kind: "migration" | "ghost";
  label?: string;
}

export function SchemaDiagramCanvas({
  nodes,
  edges,
  emptyState,
}: {
  nodes: SchemaTableNodeView[];
  edges: SchemaTableReferenceView[];
  emptyState: string;
}) {
  const [flowNodes, setFlowNodes] = useState<Node<SchemaTableNodeView>[]>([]);
  const [flowEdges, setFlowEdges] = useState<Edge[]>([]);

  useEffect(() => {
    let active = true;

    if (nodes.length === 0) {
      setFlowNodes([]);
      setFlowEdges([]);
      return;
    }

    void layoutNodes(
      nodes.map((node) => ({
        id: node.id,
        width: TABLE_NODE_WIDTH,
        height: tableNodeHeight(node),
      })),
      edges.map((edge) => ({
        id: edge.id,
        source: edge.sourceTable,
        target: edge.targetTable,
      })),
      "RIGHT",
    ).then((positions) => {
      if (!active) {
        return;
      }

      setFlowNodes(
        nodes.map((node) => ({
          id: node.id,
          type: "schemaTable",
          position: positions[node.id] ?? { x: 0, y: 0 },
          draggable: false,
          data: node,
        })),
      );
      setFlowEdges(
        edges.map((edge) => ({
          id: edge.id,
          source: edge.sourceTable,
          sourceHandle: `source:${edge.sourceColumn}`,
          target: edge.targetTable,
          targetHandle: "target:table",
          label: edge.label ?? edge.sourceColumn,
          type: "smoothstep",
          markerEnd: { type: MarkerType.ArrowClosed },
          style: edgeStyle(edge.state),
          labelStyle: edgeLabelStyle(edge.state),
        })),
      );
    });

    return () => {
      active = false;
    };
  }, [edges, nodes]);

  if (nodes.length === 0) {
    return <div className={styles.emptyState}>{emptyState}</div>;
  }

  return (
    <div className={styles.canvas}>
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={{ schemaTable: SchemaTableNode }}
        fitView
        nodesConnectable={false}
        nodesDraggable={false}
        elementsSelectable={false}
        panOnDrag
        zoomOnDoubleClick={false}
      >
        <Background gap={18} color="#223044" />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) =>
            tableStateColor(
              ((node.data as SchemaTableNodeView | undefined)?.state ?? "unchanged") as Exclude<
                SchemaDiffState,
                "unknown"
              >,
            )
          }
        />
        <Controls />
      </ReactFlow>
    </div>
  );
}

export function SchemaCompatibilityGraph({
  nodes,
  edges,
}: {
  nodes: SchemaVersionNodeView[];
  edges: SchemaVersionEdgeView[];
}) {
  const [flowNodes, setFlowNodes] = useState<Node<SchemaVersionNodeView>[]>([]);
  const [flowEdges, setFlowEdges] = useState<Edge[]>([]);

  useEffect(() => {
    let active = true;

    if (nodes.length === 0) {
      setFlowNodes([]);
      setFlowEdges([]);
      return;
    }

    void layoutNodes(
      nodes.map((node) => ({
        id: node.id,
        width: HASH_NODE_WIDTH,
        height: 104,
      })),
      edges.map((edge) => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
      })),
      "RIGHT",
    ).then((positions) => {
      if (!active) {
        return;
      }

      setFlowNodes(
        nodes.map((node) => ({
          id: node.id,
          type: "schemaVersion",
          position: positions[node.id] ?? { x: 0, y: 0 },
          draggable: false,
          data: node,
        })),
      );
      setFlowEdges(
        edges.map((edge) => ({
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: "smoothstep",
          label: edge.label,
          markerEnd: { type: MarkerType.ArrowClosed },
          style:
            edge.kind === "ghost"
              ? { stroke: "#7d899c", strokeDasharray: "6 5", opacity: 0.8 }
              : { stroke: "#67c1ff", strokeWidth: 1.6 },
          labelStyle:
            edge.kind === "ghost"
              ? { fill: "#aab6c8", fontSize: 11 }
              : { fill: "#8fd5ff", fontSize: 11 },
        })),
      );
    });

    return () => {
      active = false;
    };
  }, [edges, nodes]);

  if (nodes.length === 0) {
    return <div className={styles.emptyState}>No schema graph data available.</div>;
  }

  return (
    <div className={styles.canvas}>
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={{ schemaVersion: SchemaVersionNode }}
        fitView
        nodesConnectable={false}
        nodesDraggable={false}
        elementsSelectable={false}
        panOnDrag
        zoomOnDoubleClick={false}
      >
        <Background gap={18} color="#223044" />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) =>
            (node.data as SchemaVersionNodeView | undefined)?.selected ? "#8ed7ff" : "#506177"
          }
        />
        <Controls />
      </ReactFlow>
    </div>
  );
}

function SchemaTableNode({ data }: NodeProps<Node<SchemaTableNodeView, "schemaTable">>) {
  const columnRows = data.columns;

  return (
    <div className={`${styles.tableNode} ${tableStateClassName(data.state)}`}>
      <Handle
        type="target"
        id="target:table"
        position={Position.Left}
        className={styles.tableTargetHandle}
      />
      <div className={styles.tableHeader}>
        <div>
          <div className={styles.tableName}>{data.tableName}</div>
          {data.subtitle ? <div className={styles.tableSubtitle}>{data.subtitle}</div> : null}
        </div>
        <div className={styles.tableCount}>{columnRows.length} cols</div>
      </div>
      <div className={styles.columnList}>
        {columnRows.map((column, index) => (
          <div
            key={`${data.tableName}:${column.name}:${index}`}
            className={`${styles.columnRow} ${columnStateClassName(column.state)}`}
          >
            <div className={styles.columnName}>{column.name}</div>
            <div className={styles.columnMeta}>
              <span>{column.typeLabel}</span>
              <span>{column.nullable ? "nullable" : "required"}</span>
            </div>
            {column.reference ? (
              <div className={styles.columnReference}>ref {column.reference}</div>
            ) : null}
            {column.reference ? (
              <Handle
                type="source"
                id={`source:${column.name}`}
                position={Position.Right}
                className={styles.columnSourceHandle}
                style={{ top: 76 + index * 56 }}
              />
            ) : null}
          </div>
        ))}
      </div>
      {data.unknownMappings?.length ? (
        <div className={styles.unknownMappings}>
          {data.unknownMappings.map((mapping) => (
            <div
              key={`${data.tableName}:${mapping.fromColumn}:${mapping.toColumn}`}
              className={styles.unknownMappingRow}
            >
              Unknown mapping: {mapping.fromColumn} {"->"} {mapping.toColumn}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function SchemaVersionNode({ data }: NodeProps<Node<SchemaVersionNodeView, "schemaVersion">>) {
  return (
    <div className={`${styles.hashNode} ${data.selected ? styles.hashNodeSelected : ""}`}>
      <div className={styles.hashValue}>{shortHash(data.hash)}</div>
      <div className={styles.hashMeta}>
        <span>{data.stats.tableCount} tables</span>
        <span>{data.stats.columnCount} columns</span>
        <span>{data.stats.referenceCount} refs</span>
      </div>
      {data.selected ? <div className={styles.hashBadge}>selected</div> : null}
    </div>
  );
}

function tableNodeHeight(node: SchemaTableNodeView): number {
  const baseHeight = 84;
  const unknownMappingsHeight = (node.unknownMappings?.length ?? 0) * 26;
  return baseHeight + node.columns.length * 56 + unknownMappingsHeight;
}

async function layoutNodes(
  nodes: Array<{ id: string; width: number; height: number }>,
  edges: Array<{ id: string; source: string; target: string }>,
  direction: "RIGHT" | "DOWN",
) {
  const graph = await elk.layout({
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": direction,
      "elk.spacing.nodeNode": "48",
      "elk.layered.spacing.nodeNodeBetweenLayers": "96",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
    },
    children: nodes.map((node) => ({
      id: node.id,
      width: node.width,
      height: node.height,
    })),
    edges: edges.map((edge) => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  });

  return Object.fromEntries(
    (graph.children ?? []).map((node) => [node.id!, { x: node.x ?? 0, y: node.y ?? 0 }]),
  ) as Record<string, { x: number; y: number }>;
}

function edgeStyle(state: Exclude<SchemaDiffState, "unknown">) {
  if (state === "added") {
    return { stroke: "#5ad08c", strokeWidth: 1.6 };
  }
  if (state === "removed") {
    return { stroke: "#ff9a9a", strokeDasharray: "6 4", strokeWidth: 1.6 };
  }
  if (state === "changed") {
    return { stroke: "#ffd166", strokeWidth: 1.6 };
  }
  return { stroke: "#6ab9ff", strokeWidth: 1.4 };
}

function edgeLabelStyle(state: Exclude<SchemaDiffState, "unknown">) {
  if (state === "added") {
    return { fill: "#79dca1", fontSize: 11 };
  }
  if (state === "removed") {
    return { fill: "#ffb0b0", fontSize: 11 };
  }
  if (state === "changed") {
    return { fill: "#ffd166", fontSize: 11 };
  }
  return { fill: "#99c8f0", fontSize: 11 };
}

function tableStateClassName(state: Exclude<SchemaDiffState, "unknown">) {
  if (state === "added") {
    return styles.tableNodeAdded;
  }
  if (state === "removed") {
    return styles.tableNodeRemoved;
  }
  if (state === "changed") {
    return styles.tableNodeChanged;
  }
  return "";
}

function columnStateClassName(state: SchemaDiffState) {
  if (state === "added") {
    return styles.columnAdded;
  }
  if (state === "removed") {
    return styles.columnRemoved;
  }
  if (state === "changed") {
    return styles.columnChanged;
  }
  if (state === "unknown") {
    return styles.columnUnknown;
  }
  return "";
}

function tableStateColor(state: Exclude<SchemaDiffState, "unknown">) {
  if (state === "added") {
    return "#3c8b5f";
  }
  if (state === "removed") {
    return "#8a4b4b";
  }
  if (state === "changed") {
    return "#9b7b2e";
  }
  return "#35506f";
}
