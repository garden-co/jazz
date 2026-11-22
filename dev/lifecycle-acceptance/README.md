# Native-client lifecycle acceptance

Runs open, persist, sign out and in, cancel and close in Rust, Swift (Linux), Kotlin/JVM and Go on
**a candidate's shipped artifacts**, as an adopter gets them, plus the Android AAR on an emulator.
It prints `PASS`, `FAIL` or `NOT_COVERED` per rule and language.

## Run it on a candidate

```bash
dev/lifecycle-acceptance/run.sh <candidate sha> <release-artifacts run id> [name]
```

It commits the candidate to `candidates.json` on `eval/lifecycle-acceptance` (the only thing it
pushes), waits for the workflow, prints the verdict and the wall time, and exits 0 only if the
candidate passed and every negative control failed as it must. Once the workflow is on main:

```bash
gh workflow run lifecycle-acceptance.yml -f sha=<sha> -f run=<run id>
```

The release-artifacts run must have built exactly that sha, on a push or a dispatch in this
repository. Anything else stops the job before a test runs.

## What is tested is what ships

| consumer | gets | can't fall back because |
| --- | --- | --- |
| backend | `release-npm` tarballs, `release-server`'s core binary | the install's registry answers 404 for every Jazz name; installed files are compared with the tarballs |
| Rust | `.crate` files packaged from the candidate as the publish does, `[patch.crates-io]` | the build stops first if any Jazz crate resolves to a registry or another path |
| Swift | `release-swift-mirror` as a tagged package, `release-languages`' libjazz_ffi through pkg-config | the checkout is compared file by file; the loaded library's sha256 is checked |
| Kotlin/JVM | `tools.jazz:jazz-kotlin` from `release-maven` | `exclusiveContent` for tools.jazz; the jar's sha256 is checked against the manifest |
| Go | the module with `release-languages`' libraries embedded, as release-publish makes it | the build has only file proxies; the zip, the embedded and the loaded library are checked |
| Android | `release-maven`'s AAR, through jazz#6246's test at `android.json`'s sha | the AAR's sha256 is checked against the manifest |

main can have the same version as the last release, so a version number proves nothing here: every
check is by sha256. Consumers are projects outside the checkout, and their bindings come from the
candidate's own `jazz-tools generate`.

## Reading the result

- `PASS`: every round passed. Nothing is retried except network fetches: a flake fails.
- `FAIL`: at least one round broke the rule, with the count and the first example. A missing
  result is a FAIL too.
- `NOT_COVERED`: the language has no API for it, the workflow doesn't cover it, or the contract
  doesn't decide it yet (`pending`: the line says what was observed).
- The verdict is `INCOMPLETE`, never `PASS`, while a rule is pending (awaiting a decision) or covered elsewhere.
- `basis`: `contract` rules are decided by #native-language-apis and scored; `pending` ones are
  awaiting Anselm and report what they observed.
- Android: one line per rule from jazz#6246's test at `android.json`'s pin (marked unreviewed
  until it is). `BLOCKED` is a FAIL. Its store rules report what they observed while awaiting a decision.
- `CONTROL_BROKEN`: a candidate in `candidates.json` known to break a rule (`mustFail`) didn't. The
  harness is then broken, whatever else it says.

Every check has a time limit and one expected outcome, matched by error kind: an error that merely
happened is never a PASS.

## Change it

- A rule is one entry in `suite/rules/*.mjs`, written once for all languages.
- A new operation goes into `drivers/PROTOCOL.md` and each driver. Drivers judge nothing.
- `node --test dev/lifecycle-acceptance/test/suite.test.mjs` tests the harness itself.
- Locally: `LA_WORK=<dir outside any checkout> LA_SOURCE=<candidate checkout>`, then
  `node fetch.mjs <sha> <run> <artifacts>`, `node build.mjs <lang>`,
  `node suite/run.mjs --lang <lang> [--only <id prefix>] [--rounds n]`.
