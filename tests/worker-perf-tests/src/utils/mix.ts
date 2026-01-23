export type MixMode = "round_robin" | "randomized";

export type MixSpec = {
  files: number;
  maps: number;
  mode: MixMode;
};

export function parseMixSpec(mix: string, mode: MixMode): MixSpec {
  const m = mix.trim().match(/^(\d+)f:(\d+)m$/i);
  if (!m) {
    throw new Error(
      `Invalid --mix "${mix}". Expected format like "1f:1m" or "2f:1m".`,
    );
  }
  const files = Number(m[1]);
  const maps = Number(m[2]);
  if (
    !Number.isFinite(files) ||
    !Number.isFinite(maps) ||
    files <= 0 ||
    maps <= 0
  ) {
    throw new Error(
      `Invalid --mix "${mix}". X and Y must be positive integers.`,
    );
  }
  return { files, maps, mode };
}

export type OpKind = "file" | "map";

export function makeMixCycle(spec: MixSpec, rng: () => number): OpKind[] {
  const cycle: OpKind[] = [];
  for (let i = 0; i < spec.files; i++) cycle.push("file");
  for (let i = 0; i < spec.maps; i++) cycle.push("map");

  if (spec.mode === "randomized") {
    // Fisher–Yates shuffle
    for (let i = cycle.length - 1; i > 0; i--) {
      const j = Math.floor(rng() * (i + 1));
      [cycle[i], cycle[j]] = [cycle[j]!, cycle[i]!];
    }
  }

  return cycle;
}
