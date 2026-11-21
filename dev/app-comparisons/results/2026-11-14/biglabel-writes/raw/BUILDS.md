# Which build each raw directory comes from

Directory names: `<variant>[.fix-<app/jazz knobs>].<tag>.marks`. The knobs are `localStorage["bl-fix"]`, so
one build holds several. Tags name the build:

| tag | build (big-label/jazz, jazz-tools 2.0.0-alpha.62 dist) | order |
|---|---|---|
| `bodies`, `try`, (none), `profile` | pristine dist, upstream app (before the knobs existed). `bodies` records frame contents | next, update, cross |
| `a` | pristine dist + app knobs (`no-precheck`, `precheck-local`, `tx-local`) | update, cross |
| `b` | + dist `sync-outbound-first`, `tx-read-local` (both gated by the knob) | update, cross |
| `c`, `c1`–`c3` | b + `debounce0` (not gated: every sample in these builds has it) | next, update, cross |
| `d1`, `d2` | b + server route timing (`BL_ROUTE_TIMING=1`) | next, update, cross |
| `t1`, `t2` (thin) | thin, route timing | next, update, cross |
| `cookiecache1/2` (thin) | thin, route timing, `BL_AUTH_COOKIE_CACHE=1` | next, update, cross |
| `cookiecache-e1/e2` | b + `BL_AUTH_COOKIE_CACHE=1` | next, update, cross |
| `cookiecache-w1/w2` | same build | update, cross (no paging first) |
| `cookiecache-gap0/gap31000` | same build; `--gap`: leave the list and wait after paging | next, (gap), update, cross |

`../logs/sw*.log` are the sweep outputs (one line per run) in the same order as above: sw1/sw2 = a,
sw3 = b, sw4 = c, sw5 = d, sw6 = thin t, sw7 = thin cookiecache, sw8 = cookiecache-e/w.
