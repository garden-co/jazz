export type CorrectnessArtifactSnapshot = Readonly<{
  schema: 1;
  fingerprint: string;
  wasmFingerprint: string;
  napiFingerprint: string;
  wasmPackage: string;
  napiGeneration: string;
  files: Readonly<Record<string, string>>;
}>;

export function correctnessArtifactStore(root: string): string;
export function correctnessArtifactPointer(root: string): string;
export function snapshotCorrectnessArtifacts(root: string): CorrectnessArtifactSnapshot;
export function readCorrectnessArtifactSnapshot(root: string): CorrectnessArtifactSnapshot | null;
