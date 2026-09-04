# INV-SYNC-36

- Status: target
- Coverage: untested

## Invariant

Peer sync carries an exact authorized input closure, never authority-produced application terminal rows, collector positions, or ordered terminal operations. A receiver MUST reconcile admitted authority inputs with tier-eligible local inputs and derive the only application terminal by running the same local maintained Groove program used for local changes. A strict remote read uses only its fresh exact authority closure; a local-first read may additionally include eligible local pending inputs. Settlement follows complete witness admission and local IVM quiescence.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned covered-input reconciliation, receiver-local maintained IVM feed, terminal-wire retirement, and transient exact-coverage one-shot lifecycle
