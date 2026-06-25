// Dev-only: renders the inspector overlay loader script for Next.js apps.
//
// Next has no `transformIndexHtml`-style hook (unlike Vite/SvelteKit), so the
// loader can't be auto-injected. Instead, Next users add this component once to
// their root layout. withJazz (./next.ts) copies the loader + embedded build
// into the app's `public/__jazz/` during dev, so `/__jazz/loader.js` resolves.
//
// In production this returns null, and bundlers statically drop the branch
// because `process.env.NODE_ENV` is replaced at build time. It's a plain script
// tag with no hooks or client state, so it's safe to render from a Server
// Component.
export function JazzInspectorScript() {
  if (process.env.NODE_ENV === "production") return null;
  return <script src="/__jazz/loader.js" />;
}
