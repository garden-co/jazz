export type BenchmarkDurabilityTier = "local" | "edge" | "global";
export type BenchmarkSyncSettlementTier = "edge" | "global";
export type BenchmarkWriteHandle = {
  wait(options: { tier: BenchmarkDurabilityTier }): Promise<unknown>;
};

function nowMs(): number {
  return performance.now();
}

export async function waitForBenchmarkWriteDurability(
  handles: BenchmarkWriteHandle[],
  syncSettlementTier: BenchmarkSyncSettlementTier | undefined,
  setStatus: (status: string) => void,
): Promise<{
  localDurabilityMs: number;
  syncSettlementMs?: number;
  syncSettlementTier?: BenchmarkSyncSettlementTier;
}> {
  setStatus("wait-local-durability");
  const localDurabilityStart = nowMs();
  await Promise.all(handles.map((handle) => handle.wait({ tier: "local" })));
  const localDurabilityMs = nowMs() - localDurabilityStart;

  if (!syncSettlementTier) {
    return { localDurabilityMs };
  }

  setStatus("wait-sync-settlement");
  const syncSettlementStart = nowMs();
  await Promise.all(handles.map((handle) => handle.wait({ tier: syncSettlementTier })));
  const syncSettlementMs = nowMs() - syncSettlementStart;

  return { localDurabilityMs, syncSettlementMs, syncSettlementTier };
}
