# jazz-rn-android

## 2.0.0-alpha.58

### Patch Changes

- 94e3090: Keep native client relay ticks recoverable after temporary socket closure and HTTP connection failures (408, 425, 429, 500, 502, 503, 504), as well as hostname-resolution failures or TLS EOF without `close_notify`. Authentication, malformed protocol, certificate, and unclassified I/O failures remain terminal.

## 2.0.0-alpha.57

## 2.0.0-alpha.56

## 2.0.0-alpha.55

### Patch Changes

- 060083d: Split React Native binaries into exact-version iOS and Android payload dependencies, retaining all supported architectures while keeping each npm upload below the package budget.
