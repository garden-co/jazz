# jazz-rn-ios

## 2.0.0-alpha.63

## 2.0.0-alpha.62

## 2.0.0-alpha.61

## 2.0.0-alpha.60

### Patch Changes

- 0aec19c: Fix live combined aggregate subscriptions missing changes when the first aggregate value stays unchanged. Complete summaries now identify replacements without changing group occurrences or limiting large multi-output summaries.
- ff0d69b: Fix grouped aggregate results using opaque identities for default ordering and explicit-order ties. Reads and live subscriptions now use logical group keys before pagination, while preserving group occurrence identities and explicit sort directions.
- 6f292f9: Explain local foreground processing failures in the native tick exception using the existing lifecycle status, without changing the native artifact ABI.
- e432352: Fix nullable group keys having different public representations in aggregate reads and subscriptions. One-shot reads now preserve NULL group keys and use the same logical result types as snapshots and live updates.

  Fix live aggregates losing nullable groups when their source rows change. Updates now find the indexed group using the same nullable key encoding, preserving counts, sums and group identity.

## 2.0.0-alpha.59

## 2.0.0-alpha.58

### Patch Changes

- 94e3090: Keep native client relay ticks recoverable after temporary socket closure and HTTP connection failures (408, 425, 429, 500, 502, 503, 504), as well as hostname-resolution failures or TLS EOF without `close_notify`. Authentication, malformed protocol, certificate, and unclassified I/O failures remain terminal.

## 2.0.0-alpha.57

## 2.0.0-alpha.56

## 2.0.0-alpha.55

### Patch Changes

- 060083d: Split React Native binaries into exact-version iOS and Android payload dependencies, retaining all supported architectures while keeping each npm upload below the package budget.
