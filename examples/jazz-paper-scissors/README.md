# Jazz Paper Scissors

## Setup

First of we need to create a new account for the dealer:

```bash
pnpx jazz-run account create --name "Worker"
```

This will print an account ID and a secret key:

```
# Credentials for Jazz account "Dealer":
JAZZ_WORKER_ACCOUNT=co_xxxx
JAZZ_WORKER_SECRET=sealerSecret_xxx
```

use these to create a `.env` file based on the `.env.example` file and fill in the `VITE_JAZZ_WORKER_ACCOUNT` and `JAZZ_WORKER_SECRET` fields.

We can then start the dealer worker:

```bash
pnpm start:worker
```

and the client:

```bash
pnpm start
```
