# Summary: Cost-based Query Planner

## The problem in one sentence

When you ask Jazz for "the latest 10 messages", it currently loads every message in the table, sorts them all, and throws away everything except 10.

## Why it works that way today

Every query in Jazz goes through the same fixed steps. There is no "thinking" step where Jazz looks at the query and picks a smart way to run it. It just does the one thing it knows how to do.

Imagine a chef who only has one recipe for every meal. You ask for a salad, they boil it. You ask for soup, they boil it. The food comes out, but there is no choice.

## What we are building

A **planner** — a thinking step that runs before each query executes. It looks at the query, looks at what indexes are available, and picks the best way to run it.

For "the latest 10 messages", a planner can do this:

1. Notice there is an index on `created_at`.
2. Pick a plan that walks the index backwards and stops after 10 rows.
3. Load only those 10 rows. The other 9 990 are never touched.

Same query. Same result. Much less work.

## How the planner picks

Two ingredients:

1. **Multiple plans for the same query.** Instead of one fixed pipeline, the planner generates several candidate plans. Each is valid; some are faster than others.
2. **Cost estimates.** The planner predicts how much work each plan does and picks the cheapest. The estimates come from exact index counts — we already know how many rows have `channel_id = X`, no guessing.

## Why this is a foundation, not just one fix

The top-k speedup is the first win. But every future optimization needs the same thing: a place where Jazz can choose between alternatives.

Once the planner exists:

- Top-k queries get fast (the immediate win)
- Pagination gets fast (resume from a cursor instead of re-sorting)
- Joins get smarter (pick the right algorithm for each shape)
- Future ideas have a place to live

Without a planner, every new optimization becomes a special case bolted onto the existing pipeline. With one, they all share the same machinery.

## What we are NOT building

- No statistical guesses. We have exact counts, no histograms.
- No way for users to override the planner. It decides automatically.
- No `EXPLAIN` command for end users — internal debug tool only.
- No re-planning while a subscription is running. Plan once, stick with it.

## Rollout

Five phases, each ships on its own:

1. **Foundations.** Build the planner module, but make it produce the same plans as today. Nothing changes for users. Sanity check that the new pipes carry water.
2. **Storage extensions.** Add the storage methods the planner needs (ordered scan, count rows, count distinct values).
3. **Single-table optimizations.** Turn the planner on for single-table queries. Top-k speedup lands here.
4. **Joins.** Extend the planner to multi-table queries. Faster joined feeds land here.
5. **Cleanup.** Delete the old code paths.
