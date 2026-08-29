export type CorrectnessArtifactSnapshot = Readonly<{
  schema: 2;
  fingerprint: string;
  wasmFingerprint: string;
  napiFingerprint: string;
  cliFingerprint: string;
  wasmPackage: string;
  napiGeneration: string;
  cliArtifact: string;
  files: Readonly<Record<string, string>>;
}>;

export function correctnessArtifactStore(root: string): string;
export function correctnessArtifactPointer(root: string): string;
export function snapshotCorrectnessArtifacts(root: string): CorrectnessArtifactSnapshot;
export function readCorrectnessArtifactSnapshot(root: string): CorrectnessArtifactSnapshot | null;
export function readCorrectnessArtifactSnapshotByFingerprint(
  root: string,
  fingerprint: string,
): CorrectnessArtifactSnapshot;
