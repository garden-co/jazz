// Factoring of disjunctive policy branches back into a conjunction of disjunctions.
import { Writer, toHex } from "../../../support/index.js";
import {
  encodePredicate,
  encodeJoinVia,
  encodeReachableVia,
  encodeInheritsVia,
} from "../../../_extern/node-qe-a.js";
import type { PolicyBranch } from "../../../_extern/node-qe-a.js";

type Atom =
  | { kind: "Filter"; v: any }
  | { kind: "Join"; v: any }
  | { kind: "Reachable"; v: any }
  | { kind: "Inherits"; v: any };

const setKey = (s: Iterable<number>) => [...s].sort((a, b) => a - b).join(",");

export function factorPolicyBranches(branches: PolicyBranch[]): PolicyBranch[][] | undefined {
  if (branches.length < 4) return undefined;
  const atoms: [string, Atom][] = [];
  const rows: Set<number>[] = [];
  for (const branch of branches) {
    const row = new Set<number>();
    for (const atom of branchAtoms(branch)) {
      const key = atomKey(atom);
      if (key === undefined) return undefined;
      let index = atoms.findIndex(([existing]) => existing === key);
      if (index < 0) {
        atoms.push([key, atom]);
        index = atoms.length - 1;
      }
      row.add(index);
    }
    rows.push(row);
  }
  if (new Set(rows.map(setKey)).size !== rows.length) return undefined;

  const parent = atoms.map((_, i) => i);
  for (let left = 0; left < atoms.length; left++) {
    for (let right = left + 1; right < atoms.length; right++) {
      const joint = new Set(rows.map((row) => `${row.has(left)}|${row.has(right)}`));
      const lp = new Set([...joint].map((j) => j.split("|")[0]));
      const rp = new Set([...joint].map((j) => j.split("|")[1]));
      if (joint.size !== lp.size * rp.size) {
        const a = find(parent, left);
        const b = find(parent, right);
        parent[a] = b;
      }
    }
  }
  const groups: [number, Set<number>][] = [];
  for (let atom = 0; atom < atoms.length; atom++) {
    const root = find(parent, atom);
    const g = groups.find(([r]) => r === root);
    if (g) g[1].add(atom);
    else groups.push([root, new Set([atom])]);
  }
  const factors = groups.map(([, members]) => {
    const alts = new Map<string, number[]>();
    for (const row of rows) {
      const inter = [...row].filter((i) => members.has(i)).sort((a, b) => a - b);
      alts.set(inter.join(","), inter);
    }
    return [...alts.entries()]
      .sort(([a], [b]) => cmpIdxList(alts.get(a)!, alts.get(b)!))
      .map(([, v]) => v);
  });
  if (factors.length < 2 || factors.every((alts) => alts.length < 2)) return undefined;
  let productSize = 1;
  for (const alts of factors) {
    productSize *= alts.length;
    if (!Number.isSafeInteger(productSize)) return undefined;
  }
  if (productSize !== rows.length) return undefined;
  return factors.map((alts) => alts.map((members) => branchFromAtoms(atoms, members)));
}

function cmpIdxList(a: number[], b: number[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
}

function find(parent: number[], index: number): number {
  while (parent[index] !== index) {
    parent[index] = parent[parent[index]];
    index = parent[index];
  }
  return index;
}

function branchAtoms(branch: PolicyBranch): Atom[] {
  return [
    ...branch.filters.map((v: any): Atom => ({ kind: "Filter", v })),
    ...branch.joins.map((v: any): Atom => ({ kind: "Join", v })),
    ...branch.reachable.map((v: any): Atom => ({ kind: "Reachable", v })),
    ...branch.inherits.map((v: any): Atom => ({ kind: "Inherits", v })),
  ];
}

function atomKey(atom: Atom): string | undefined {
  try {
    const w = new Writer();
    switch (atom.kind) {
      case "Filter":
        encodePredicate(w.u8(0), atom.v);
        break;
      case "Join":
        encodeJoinVia(w.u8(1), atom.v);
        break;
      case "Reachable":
        encodeReachableVia(w.u8(2), atom.v);
        break;
      case "Inherits":
        encodeInheritsVia(w.u8(3), atom.v);
        break;
    }
    return toHex(w.finish());
  } catch {
    return undefined;
  }
}

function branchFromAtoms(atoms: [string, Atom][], members: number[]): PolicyBranch {
  const branch: PolicyBranch = { filters: [], joins: [], reachable: [], inherits: [] };
  for (const index of members) {
    const atom = atoms[index][1];
    switch (atom.kind) {
      case "Filter":
        branch.filters.push(atom.v);
        break;
      case "Join":
        branch.joins.push(atom.v);
        break;
      case "Reachable":
        branch.reachable.push(atom.v);
        break;
      case "Inherits":
        branch.inherits.push(atom.v);
        break;
    }
  }
  return branch;
}
