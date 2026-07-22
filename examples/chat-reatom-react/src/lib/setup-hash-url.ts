import { onEvent, urlAtom } from "@reatom/core";

export const hashToUrl = () =>
  new URL([window.origin, window.location.hash.replace(/^#\//, "")].join("/"));

export const pathToHash = (path: string) => `#${path}`;

export const setupHashUrl = () => {
  urlAtom.syncFromSource(hashToUrl(), true);

  onEvent(window, "hashchange", () => urlAtom.syncFromSource(hashToUrl(), true));

  urlAtom.sync.set(() => (url, replace) => {
    const path = url.href.replace(window.origin, "");
    if (replace) {
      history.replaceState({}, "", pathToHash(path));
    } else {
      history.pushState({}, "", pathToHash(path));
    }
  });
};
