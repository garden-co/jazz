import { describe, expect, it } from "vitest";

import { type Anchors, connectChain, gridPlacement, routeEdges } from "./geometry";

// Build an Anchors rect directly (bypassing DOM/insetRect) so the routing
// maths is tested in isolation — this is abstract graph mechanics, so terse
// node ids are appropriate.
function rect(left: number, right: number, top: number, bottom: number): Anchors {
  return {
    left,
    right,
    top,
    bottom,
    midX: (left + right) / 2,
    midY: (top + bottom) / 2,
  };
}

const byId = (edges: ReturnType<typeof routeEdges>, id: string) => {
  const e = edges.find((x) => x.id === id);
  if (!e) throw new Error(`no edge ${id}`);
  return e;
};

describe("routeEdges — single orthogonal hop", () => {
  it("routes a vertically-aligned downward edge as a clean elbow (TD)", () => {
    const anchors = {
      A: rect(0, 100, 0, 20),
      B: rect(0, 100, 60, 80),
    };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");

    expect(e.id).toBe("A->B");
    expect(e.source).toEqual({ x: 50, y: 20 }); // A.bottom
    expect(e.target).toEqual({ x: 50, y: 60 }); // B.top (downward enters the top)
    // armY = (A.bottom + B.top) / 2 = 40; collinear points collapse.
    expect(e.d).toBe("M 50 20 L 50 40 L 50 60");
    expect(e.length).toBeCloseTo(40);
  });

  it("exits the source top / enters target bottom for an upward edge (TD)", () => {
    const anchors = {
      Lo: rect(0, 100, 60, 80),
      Hi: rect(0, 100, 0, 20),
    };
    const [e] = routeEdges(anchors, [{ from: "Lo", to: "Hi" }], "TD");

    expect(e.source).toEqual({ x: 50, y: 60 }); // Lo.top
    expect(e.target).toEqual({ x: 50, y: 20 }); // Hi.bottom
    expect(e.d).toBe("M 50 60 L 50 40 L 50 20");
    expect(e.length).toBeCloseTo(40);
  });

  it("routes left-to-right along the cross axis (LR)", () => {
    const anchors = {
      A: rect(0, 20, 0, 100),
      B: rect(60, 80, 0, 100),
    };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "LR");

    expect(e.source).toEqual({ x: 20, y: 50 }); // A.right
    expect(e.target).toEqual({ x: 60, y: 50 }); // B.left
    expect(e.d).toBe("M 20 50 L 40 50 L 60 50");
    expect(e.length).toBeCloseTo(40);
  });
});

describe("routeEdges — laned fork/merge (no overlap, crossing-minimised)", () => {
  it("spreads a fork across the source side, ordered by target cross-position", () => {
    const anchors = {
      S: rect(0, 100, 0, 20),
      T1: rect(0, 40, 60, 80), // midX 20 (left)
      T2: rect(60, 100, 60, 80), // midX 80 (right)
    };
    const edges = routeEdges(
      anchors,
      [
        { from: "S", to: "T2" },
        { from: "S", to: "T1" },
      ],
      "TD",
    );

    const toT1 = byId(edges, "S->T1");
    const toT2 = byId(edges, "S->T2");

    // Distinct exit points on S's bottom side, both at S.bottom.
    expect(toT1.source.y).toBe(20);
    expect(toT2.source.y).toBe(20);
    expect(toT1.source.x).not.toBeCloseTo(toT2.source.x);
    // Ordered by the target's x so the lines don't cross.
    expect(toT1.source.x).toBeLessThan(toT2.source.x);
    // Two edges over S's [0,100] span ⇒ thirds.
    expect(toT1.source.x).toBeCloseTo(100 / 3);
    expect(toT2.source.x).toBeCloseTo(200 / 3);
    // Each enters its (single-edge) target at its mid.
    expect(toT1.target.x).toBeCloseTo(20);
    expect(toT2.target.x).toBeCloseTo(80);
  });

  it("spreads a merge across the target side, ordered by source cross-position", () => {
    const anchors = {
      T1: rect(0, 40, 0, 20), // midX 20
      T2: rect(60, 100, 0, 20), // midX 80
      M: rect(0, 100, 60, 80),
    };
    const edges = routeEdges(
      anchors,
      [
        { from: "T2", to: "M" },
        { from: "T1", to: "M" },
      ],
      "TD",
    );

    const fromT1 = byId(edges, "T1->M");
    const fromT2 = byId(edges, "T2->M");

    expect(fromT1.target.y).toBe(60); // M.top
    expect(fromT2.target.y).toBe(60);
    expect(fromT1.target.x).toBeLessThan(fromT2.target.x);
    expect(fromT1.target.x).toBeCloseTo(100 / 3);
    expect(fromT2.target.x).toBeCloseTo(200 / 3);
  });

  it("preserves input edge order in the output", () => {
    const anchors = { A: rect(0, 20, 0, 20), B: rect(0, 20, 60, 80) };
    const edges = routeEdges(
      anchors,
      [
        { from: "A", to: "B" },
        { from: "B", to: "A" },
      ],
      "TD",
    );
    expect(edges.map((e) => e.id)).toEqual(["A->B", "B->A"]);
  });
});

describe("routeEdges — converge (git-graph fork/merge)", () => {
  it("collapses a merge to a single shared anchor on the target side", () => {
    const anchors = {
      T1: rect(0, 40, 0, 20),
      T2: rect(60, 100, 0, 20),
      M: rect(0, 100, 60, 80), // midX 50
    };
    const edges = routeEdges(
      anchors,
      [
        { from: "T1", to: "M" },
        { from: "T2", to: "M" },
      ],
      "TD",
      { converge: true },
    );
    const a = byId(edges, "T1->M").target;
    const b = byId(edges, "T2->M").target;
    expect(a).toEqual(b); // both enter M at the same point
    expect(a).toEqual({ x: 50, y: 60 }); // M side midpoint (top, midX)
  });

  it("collapses a fork to a single shared anchor on the source side", () => {
    const anchors = {
      S: rect(0, 100, 0, 20), // midX 50
      A: rect(0, 40, 60, 80),
      B: rect(60, 100, 60, 80),
    };
    const edges = routeEdges(
      anchors,
      [
        { from: "S", to: "A" },
        { from: "S", to: "B" },
      ],
      "TD",
      { converge: true },
    );
    const a = byId(edges, "S->A").source;
    const b = byId(edges, "S->B").source;
    expect(a).toEqual(b); // both leave S from the same point
    expect(a).toEqual({ x: 50, y: 20 }); // S side midpoint (bottom, midX)
  });
});

describe("routeEdges — rounded corners", () => {
  it("rounds genuine bends with quadratic arcs, leaving endpoints exact", () => {
    const anchors = {
      A: rect(0, 40, 0, 20), // midX 20
      B: rect(60, 100, 60, 80), // midX 80
    };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");

    expect(e.source).toEqual({ x: 20, y: 20 });
    expect(e.target).toEqual({ x: 80, y: 60 });
    // armY = (20 + 60) / 2 = 40; two real corners at (20,40) and (80,40),
    // each rounded with r=8 (default).
    expect(e.d).toBe("M 20 20 L 20 32 Q 20 40 28 40 L 72 40 Q 80 40 80 48 L 80 60");
    expect(e.d).not.toContain("L 20 40");
  });

  it("leaves a collinear (no-bend) path as straight segments — no arcs", () => {
    const anchors = { A: rect(0, 100, 0, 20), B: rect(0, 100, 60, 80) };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");
    expect(e.d).toBe("M 50 20 L 50 40 L 50 60");
    expect(e.d).not.toContain("Q");
  });

  it("clamps the corner radius to half the shorter adjacent segment", () => {
    const anchors = {
      A: rect(0, 40, 0, 20),
      B: rect(60, 100, 30, 50), // arm at y=25 ⇒ 5px stubs ⇒ r clamps to 2.5
    };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");
    expect(e.d).toContain("Q 20 25 22.5 25");
    expect(e.d.startsWith("M 20 20")).toBe(true);
    expect(e.d.endsWith("L 80 30")).toBe(true);
  });

  it("honours an explicit cornerRadius", () => {
    const anchors = { A: rect(0, 40, 0, 20), B: rect(60, 100, 60, 80) };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD", {
      cornerRadius: 4,
    });
    expect(e.d).toBe("M 20 20 L 20 36 Q 20 40 24 40 L 76 40 Q 80 40 80 44 L 80 60");
  });
});

describe("routeEdges — reversed traversal", () => {
  it("exposes the same line traversed from target back to source", () => {
    const anchors = {
      A: rect(0, 40, 0, 20),
      B: rect(60, 100, 60, 80),
    };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");
    expect(e.d).toBe("M 20 20 L 20 32 Q 20 40 28 40 L 72 40 Q 80 40 80 48 L 80 60");
    // Same geometry, drawn from B to A (so an upward trace follows the
    // identical visible line).
    expect(e.reverse).toBe("M 80 60 L 80 48 Q 80 40 72 40 L 28 40 Q 20 40 20 32 L 20 20");
  });

  it("reverse of a straight edge is just the reversed straight line", () => {
    const anchors = { A: rect(0, 100, 0, 20), B: rect(0, 100, 60, 80) };
    const [e] = routeEdges(anchors, [{ from: "A", to: "B" }], "TD");
    expect(e.d).toBe("M 50 20 L 50 40 L 50 60");
    expect(e.reverse).toBe("M 50 60 L 50 40 L 50 20");
  });
});

describe("connectChain — one continuous path through an ordered subset", () => {
  it("threads node centres with rounded elbows (LR)", () => {
    const anchors = {
      A: rect(0, 40, 40, 60), // centre 20,50
      B: rect(80, 120, 40, 60), // centre 100,50 (same row as A)
      C: rect(160, 200, 120, 140), // centre 180,130
    };
    const { d, length } = connectChain(anchors, ["A", "B", "C"], "LR");
    expect(d).toBe("M 20 50 L 100 50 L 172 50 Q 180 50 180 58 L 180 130");
    expect(length).toBeCloseTo(240);
  });

  it("returns empty for fewer than two resolvable anchors", () => {
    expect(connectChain({ A: rect(0, 10, 0, 10) }, ["A"], "LR")).toEqual({ d: "", length: 0 });
    expect(connectChain({}, ["X", "Y"], "TD")).toEqual({ d: "", length: 0 });
  });
});

describe("gridPlacement", () => {
  it("passes explicit slots straight through", () => {
    expect(gridPlacement({ id: "n", slot: { row: 2, col: "1 / 3" } }, "TD")).toEqual({
      gridRow: "2",
      gridColumn: "1 / 3",
    });
  });

  it("maps rank to rows for TD and columns for LR (1-based grid lines)", () => {
    expect(gridPlacement({ id: "n", rank: 0 }, "TD")).toEqual({ gridRow: "1" });
    expect(gridPlacement({ id: "n", rank: 2, order: 1 }, "TD")).toEqual({
      gridRow: "3",
      gridColumn: "2",
    });
    expect(gridPlacement({ id: "n", rank: 1 }, "LR")).toEqual({ gridColumn: "2" });
    expect(gridPlacement({ id: "n", rank: 1, order: 0 }, "LR")).toEqual({
      gridColumn: "2",
      gridRow: "1",
    });
  });
});
