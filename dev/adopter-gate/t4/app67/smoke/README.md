# Smoke test

Runs against a deployed copy of this app with fresh synthetic users (random per-run passwords) and prints PASS/FAIL per step. Exits 1 on any failure.

    npm i --no-save playwright && npx playwright install --with-deps chromium
    BASE=https://<your-deployment> node smoke/smoke.mjs

Optional baseline check: set `BASELINE_RED_EMAIL` and `BASELINE_RED_PASSWORD` to a red-team user created by an earlier release's run, to check that its rows (`red-team-63`, `red-private-63`) survived the upgrade. With `BASELINE_SEED=1`, an account that can't sign in yet is created with that password and those rows on the first run (the gate does this, so the password is whatever you put in the secret).

Steps: user A (team red) writes a team row → user B (team red) sees it, also live → user C (team blue) doesn't, and C's private row stays private → a guest (device account) writes a private row, is refused a team row, signs up, and keeps its accountId and row across a reload.
