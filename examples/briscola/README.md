# Jazz Briscola

This is a simplified implementation of the Italian card game [Briscola](https://en.wikipedia.org/wiki/Briscola), written using Jazz.

While most Jazz apps don't need workers, in this game players must not be able to see each other's cards. This is a good example of when a worker is useful. In this case, the worker acts as a dealer, revealing the cards to each player as needed.

In general this showcases how workers can be used to moderate access to coValues.

The communication between the dealer and the players is done using the [Inbox API](#), which is an abstraction over the Jazz API that allows for easy communication between workers and clients.

## Setup

First of we need to create a new account for the dealer:

```bash
pnpx jazz-run account create --name "Dealer"
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
pnpm dev:worker
```

and the client:

```bash
pnpm dev
```
