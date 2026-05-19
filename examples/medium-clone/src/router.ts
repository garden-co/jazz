import { useEffect, useState } from "react";

export type Route =
  | { name: "home" }
  | { name: "edit"; draftId: string }
  | { name: "view"; articleId: string };

function parseHash(hash: string): Route {
  const cleaned = hash.replace(/^#\/?/, "");
  if (cleaned.startsWith("edit/")) {
    return { name: "edit", draftId: cleaned.slice("edit/".length) };
  }
  if (cleaned.startsWith("view/")) {
    return { name: "view", articleId: cleaned.slice("view/".length) };
  }
  return { name: "home" };
}

export function useRoute(): [Route, (next: Route) => void] {
  const [route, setRoute] = useState<Route>(() => parseHash(window.location.hash));

  useEffect(() => {
    const onHashChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  const navigate = (next: Route) => {
    const next_hash =
      next.name === "home"
        ? "#/"
        : next.name === "edit"
          ? `#/edit/${next.draftId}`
          : `#/view/${next.articleId}`;
    if (window.location.hash !== next_hash) {
      window.location.hash = next_hash;
    } else {
      setRoute(next);
    }
  };

  return [route, navigate];
}
