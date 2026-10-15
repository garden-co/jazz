// Bidirectional mapping between stable node UUIDs and compact on-disk aliases.
import { SortedMap } from "../../support/index.js";
import { type NodeAlias, type NodeUuid, cmpNodeUuid } from "../../_extern/node-state.js";

/** The in-memory `jazz_nodes` catalogue; both directions form a bijection. */
export class NodeAliases {
  private byNode = new SortedMap<NodeUuid, NodeAlias>(cmpNodeUuid);
  private byAlias = new Map<NodeAlias, NodeUuid>();
  private maxAliasValue = 0;
  private retargets = 0;

  static fromIter(entries: Iterable<[NodeUuid, NodeAlias]>): NodeAliases {
    const aliases = new NodeAliases();
    for (const [node, alias] of entries) aliases.insert(node, alias);
    return aliases;
  }

  get(node: NodeUuid): NodeAlias | undefined {
    return this.byNode.get(node);
  }

  nodeForAlias(alias: NodeAlias): NodeUuid | undefined {
    return this.byAlias.get(alias);
  }

  /** Install `node ↔ alias`, replacing any previous mapping of either side. */
  insert(node: NodeUuid, alias: NodeAlias): NodeAlias | undefined {
    this.maxAliasValue = Math.max(this.maxAliasValue, alias);
    const previousNode = this.byAlias.get(alias);
    this.byAlias.set(alias, node);
    if (previousNode !== undefined && cmpNodeUuid(previousNode, node) !== 0) {
      this.byNode.delete(previousNode);
      this.retargets++;
    }
    const previousAlias = this.byNode.set(node, alias);
    if (previousAlias !== undefined && previousAlias !== alias) {
      this.byAlias.delete(previousAlias);
      this.retargets++;
    }
    return previousAlias;
  }

  retargetCount(): number {
    return this.retargets;
  }

  maxAlias(): number {
    return this.maxAliasValue;
  }

  iter(): IterableIterator<[NodeUuid, NodeAlias]> {
    return this.byNode[Symbol.iterator]();
  }

  len(): number {
    return this.byNode.size;
  }

  equals(other: NodeAliases): boolean {
    if (this.len() !== other.len()) return false;
    for (const [node, alias] of this.byNode) if (other.get(node) !== alias) return false;
    return true;
  }

  at(node: NodeUuid): NodeAlias {
    const alias = this.byNode.get(node);
    if (alias === undefined) throw new Error("node alias is not resident");
    return alias;
  }
}
