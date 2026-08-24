# BandBinder learnings

- Recursive permission inheritance must be exercised against real sessions and
  topology, not approximated by client-side query filters.
- The product needs a deliberate concurrent-move rule before tests can assert
  a winner for two edits to the same sibling ordering.
- Large rich text and attachment bytes remain blocked on #1833, #1839, and
  #1844; ordinary page/block metadata work does not depend on them.
