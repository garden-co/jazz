# Jazz Paper Scissors

## Setup

First of we need to create a new account for the dealer:

```bash
pnpm generate-env
```

This will generate a .env file like this one

```
VITE_JAZZ_WORKER_ACCOUNT=co_zn95yzQd1z24DJCgayN53ShyuMR
JAZZ_WORKER_SECRET=sealerSecret_z3Tcq41gtELJRHk3SzQutR2DhkpvEScQQP8DG8yeSh7zJ/signerSecret_zDsLhoNRSxjXrX6oSGzGH3XQQHDyp8QS292p28RToANYq
```

This should be enough the setup everything

Then run pnpm dev to start both the local build and the worker

```bash
pnpm dev
```
