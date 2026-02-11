# Jazz 1 Migration & Community Plan — TODO (Launch)

Strategy for transitioning the community from Jazz 1 to Jazz 2 with minimal pain.

> First-week prep (repo swap, Discord setup) is in `../a_week_2026_02_09/jazz1_repo_swap.md`.

## Branding

- **Keep the name "Jazz"** — maintain brand equity, GitHub stars, followers
- Frame as a major version bump (v2), not a new product: "a big breaking change we intentionally saved for 0.x → 1.0"
- Do **not** call it "Jazz 2" in marketing — just "Jazz" with a migration guide for existing users

## Jazz 1 Support Plan

- Repos are already separate (`jazz2` is this private repo; public `jazz` is Jazz 1) — swap them at launch (see `../a_week_2026_02_09/jazz1_repo_swap.md`)
- Keep infrastructure running (cost is manageable, maintenance distraction is the concern)
- Scale down global coverage (fewer edge regions) to reduce complexity
- Migrate infra to AWS to reduce operational burden
- Continue: security updates, critical bug fixes, infrastructure support, community questions
- Dedicated Discord channel category for Jazz 1 questions
- Old documentation stays available (linked from new docs)

## Migration Path

- **E2EE is the hard part**: Jazz 1 E2EE uses group-based key management that Jazz 2 replaces entirely. Client-side migration required — users with encrypted data need to decrypt with Jazz 1 keys and re-encrypt (or store unencrypted) in Jazz 2.
- Build a reference migration of a complex Jazz 1 example app to Jazz 2
- Potentially build an LLM skill to assist with migration (code transformation)
- Acknowledge that some edge cases (inactive users, old app versions) may result in data loss
- Separate companion doc/page for existing users explaining the rationale and migration steps

## Adopter Management

- Meet with active commercial adopters early to introduce Jazz 2
- For adopters mid-rewrite on Jazz 1: evaluate case-by-case whether to continue or pivot to Jazz 2
- Internal dogfooding: port at least one adopter to Jazz 2 early to validate the migration story

## Open Questions

- How long to maintain Jazz 1 infra? (1 year? Until active users drop below threshold?)
- Should Jazz 1 adopters get priority access to Jazz 2 features they depend on?
- Community perception: how to show goodwill while being honest about the breaking change?
- E2EE migration tooling: can we automate any of it, or is it inherently manual per-app?
