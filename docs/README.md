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

GitHub owns deployments so a preview is an explicit review choice rather than
an automatic side effect of every branch. The `Docs preview (Vercel)` workflow
deploys only a trusted, same-repository pull request carrying the `docs` label.
It builds this app locally without credentials, then performs one credentialed
remote Vercel preview deployment. Label removal or closing the PR cancels an
in-flight preview; forks never receive Vercel credentials.

Before using the label, perform this external Vercel setup: create/link a
project whose Root Directory is `docs`, then disable or disconnect its automatic
Git deployments. This workflow is the sole deployment authority. Configure
these repository settings:

- Variables: `VERCEL_DOCS_ORG_ID`, `VERCEL_DOCS_PROJECT_ID`
- Secret: `VERCEL_DOCS_TOKEN`, scoped only to the docs project/team operations
  required to create preview deployments

The Vercel project's **Preview** environment must contain no sensitive values:
the labeled PR's code is built remotely. Put production-only secrets only in
Vercel's Production environment when production deployment is enabled.

Applying `docs` is a maintainer trust decision: it trusts the workflow
definition committed on that same-repository PR to upload its source with the
docs deployment token. The workflow builds the checkout without credentials in
one job, and gives a separate job only the pinned Vercel uploader plus source
checkout. This boundary reduces exposure; it cannot make a maintainer-approved
workflow change safe for sensitive Preview environment values.

Production deployment is deliberately disabled in the workflow: it has no
`main` push trigger and its production job is permanently false. At release
time, enable the documented main-only job deliberately. Do not enable Vercel's
automatic Git deployment as a substitute.
