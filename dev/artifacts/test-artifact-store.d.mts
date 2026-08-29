export type CorrectnessArtifactSnapshot = Readonly<{
  schema: 3;
  fingerprint: string;
  wasmFingerprint: string;
  napiFingerprint: string;
  wasmPackage: string;
  napiGeneration: string;
  files: Readonly<Record<string, string>>;
}>;

export function correctnessArtifactStore(root: string): string;
export function correctnessArtifactPointer(root: string): string;
export function snapshotCorrectnessArtifacts(
  root: string,
  options?: { beforePublish?: (paths: { destination: string; stage: string }) => void },
): CorrectnessArtifactSnapshot;
export function readCorrectnessArtifactSnapshot(root: string): CorrectnessArtifactSnapshot | null;
export function readCorrectnessArtifactSnapshotByFingerprint(
  root: string,
  fingerprint: string,
): CorrectnessArtifactSnapshot;
