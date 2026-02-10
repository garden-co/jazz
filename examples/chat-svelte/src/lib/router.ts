import { readable } from 'svelte/store';

function getPath(): string {
  return window.location.hash.slice(1) || window.location.pathname || '/';
}

export function navigate(href: string): void {
  const hash = href.startsWith('#') ? href : href.startsWith('/#') ? href.slice(1) : `#${href}`;
  if (window.location.hash !== hash) {
    window.location.hash = hash;
  }
}

/** Reactive path store for the app router (hash or pathname). */
export const path = readable(getPath(), (set) => {
  const handler = () => set(getPath());
  window.addEventListener('popstate', handler);
  window.addEventListener('hashchange', handler);
  return () => {
    window.removeEventListener('popstate', handler);
    window.removeEventListener('hashchange', handler);
  };
});
