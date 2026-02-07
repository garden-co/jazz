import type React from "react";
import { useEffect } from "react";
import { useRouter } from "@/hooks/useRouter";

interface Route {
  path: string;
  component: React.ComponentType<{ params?: Record<string, string> }>;
}

interface RouterProps {
  routes: Route[];
}

export default function Router({ routes }: RouterProps) {
  const { path, navigate } = useRouter();

  useEffect(() => {
    const onClick = (event: MouseEvent) => {
      const link = (event.target as HTMLElement).closest("a");
      if (
        !link ||
        event.defaultPrevented ||
        event.button !== 0 ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey
      )
        return;

      const href = link.getAttribute("href");
      if (!href || href.startsWith("http") || link.target === "_blank") return;

      event.preventDefault();

      // Logic: if it starts with #, it's a hash route. Otherwise, it's a path route.
      if (href.startsWith("#")) {
        window.location.hash = href;
      } else {
        navigate(href);
      }
    };

    window.addEventListener("click", onClick);
    return () => window.removeEventListener("click", onClick);
  }, [navigate]);

  let match = {
    Component: routes[0].component,
    params: {} as Record<string, string>,
  };

  for (const route of routes) {
    const paramNames: string[] = [];
    const regexPath = route.path.replace(/:([a-zA-Z0-9]+)/g, (_, name) => {
      paramNames.push(name);
      return "([^/]+)";
    });

    const regex = new RegExp(`^${regexPath}$`);
    const result = path.match(regex);

    if (result) {
      match = {
        Component: route.component,
        params: paramNames.reduce(
          (acc, name, index) => {
            acc[name] = decodeURIComponent(result[index + 1]);
            return acc;
          },
          {} as Record<string, string>,
        ),
      };
      break;
    }
  }

  const { Component, params } = match;
  return <Component params={params} />;
}
