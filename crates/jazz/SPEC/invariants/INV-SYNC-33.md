# INV-SYNC-33

- Status: target
- Coverage: untested

## Invariant

The serving authority MUST own visibility, membership, and settlement. It MUST ship the complete safe canonical witness closure plus identified authorized residual program for that reader/policy/binding/branch, using stable opaque admission facts rather than hidden evidence where policy disclosure would be unsafe. An opaque admission MUST be rejected if replayed across a different authority lineage/epoch, manifest, shape/binding/read view, reader/policy revision, branch/SnapshotRef, residual-program identity, protected output/source occurrence, or concrete content/deletion/witness version and layer.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned authority closure construction, residual-program identity, and policy admission facts
