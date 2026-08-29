# docs

This is a Next.js application generated with
[Create Fumadocs](https://github.com/fuma-nama/fumadocs).

Run development server:

```bash
npm run dev
# or
pnpm dev
# or
yarn dev
```

Open http://localhost:3000 with your browser to see the result.

## Explore

In the project, you can see:

- `lib/source.ts`: Code for content source adapter, [`loader()`](https://fumadocs.dev/docs/headless/source-api) provides the interface to access your content.
- `lib/layout.shared.tsx`: Shared options for layouts, optional but preferred to keep.

| Route                     | Description                                            |
| ------------------------- | ------------------------------------------------------ |
| `app/(home)`              | The route group for your landing page and other pages. |
| `app/docs`                | The documentation layout and pages.                    |
| `app/api/search/route.ts` | The Route Handler for search.                          |

### Fumadocs MDX

A `source.config.ts` config file has been included, you can customise different options like frontmatter schema.

Read the [Introduction](https://fumadocs.dev/docs/mdx) for further details.

## Learn More

To learn more about Next.js and Fumadocs, take a look at the following
resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js
  features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.
- [Fumadocs](https://fumadocs.dev) - learn about Fumadocs

## Vercel deployment

The `jazz2-docs` Vercel project is connected to this GitHub repository with
Root Directory `docs`. Its checked-in Ignored Build Step consults GitHub's
public pull-request API and builds only an open, trusted, same-repository pull
request carrying the `docs` label. Branch deployments, forks, untrusted
authors, unlabeled or closed pull requests, malformed metadata, and failed API
lookups are skipped.

Production is deliberately disabled: every production event, including a push
to `main`, exits successfully from the Ignored Build Step and does not build.
Enable production later only by changing the checked-in filter and its tests.

The Vercel project's **Preview** environment must contain no sensitive values:
applying `docs` is a maintainer trust decision that allows the pull request's
code to run in Vercel's remote build environment. No Vercel access token or
GitHub Actions secret is required for this deployment path.
