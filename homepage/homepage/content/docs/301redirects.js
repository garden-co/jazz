const frameworks = ['react', 'svelte', 'react-native', 'react-native-expo', 'vanilla'];
export const redirects = () => {
  let allRedirects = [];
  const initialSet = [
  {
    "source": "/docs/using-covalues/codecs",
    "destination": "/docs/core-concepts/covalues/codecs",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/cofeeds",
    "destination": "/docs/core-concepts/covalues/cofeeds",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/colists",
    "destination": "/docs/core-concepts/covalues/colists",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/comaps",
    "destination": "/docs/core-concepts/covalues/comaps",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/cotexts",
    "destination": "/docs/core-concepts/covalues/cotexts",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/covectors",
    "destination": "/docs/core-concepts/covalues/covectors",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/creation",
    "destination": "/docs/core-concepts/covalues/creation",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/filestreams",
    "destination": "/docs/core-concepts/covalues/filestreams",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/imagedef",
    "destination": "/docs/core-concepts/covalues/imagedef",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/imagedef/react-native-expo",
    "destination": "/docs/core-concepts/covalues/imagedef/react-native-expo",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/imagedef/react-native",
    "destination": "/docs/core-concepts/covalues/imagedef/react-native",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/imagedef/react",
    "destination": "/docs/core-concepts/covalues/imagedef/react",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/imagedef/svelte",
    "destination": "/docs/core-concepts/covalues/imagedef/svelte",
    "permanent": true
  },
  {
    "source": "/docs/schemas/covalues",
    "destination": "/docs/core-concepts/covalues/overview",
    "permanent": true
  },
  {
    "source": "/docs/schemas/accounts-and-migrations",
    "destination": "/docs/core-concepts/schemas/accounts-and-migrations",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/connecting-covalues",
    "destination": "/docs/core-concepts/schemas/connecting-covalues",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/schemaunions",
    "destination": "/docs/core-concepts/schemas/schemaunions",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/subscription-and-loading",
    "destination": "/docs/core-concepts/subscription-and-loading",
    "permanent": true
  },
  {
    "source": "/docs/sync-and-storage",
    "destination": "/docs/core-concepts/sync-and-storage",
    "permanent": true
  },
  {
    "source": "/docs/authentication/authentication-states",
    "destination": "/docs/key-features/authentication/authentication-states",
    "permanent": true
  },
  {
    "source": "/docs/authentication/better-auth-database-adapter",
    "destination": "/docs/key-features/authentication/better-auth-database-adapter",
    "permanent": true
  },
  {
    "source": "/docs/authentication/better-auth",
    "destination": "/docs/key-features/authentication/better-auth",
    "permanent": true
  },
  {
    "source": "/docs/authentication/clerk",
    "destination": "/docs/key-features/authentication/clerk",
    "permanent": true
  },
  {
    "source": "/docs/authentication/clerk/react-native",
    "destination": "/docs/key-features/authentication/clerk/react-native",
    "permanent": true
  },
  {
    "source": "/docs/authentication/overview",
    "destination": "/docs/key-features/authentication/overview",
    "permanent": true
  },
  {
    "source": "/docs/authentication/passkey",
    "destination": "/docs/key-features/authentication/passkey",
    "permanent": true
  },
  {
    "source": "/docs/authentication/passphrase",
    "destination": "/docs/key-features/authentication/passphrase",
    "permanent": true
  },
  {
    "source": "/docs/authentication/quickstart",
    "destination": "/docs/key-features/authentication/quickstart",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/history",
    "destination": "/docs/key-features/history",
    "permanent": true
  },
  {
    "source": "/docs/using-covalues/version-control",
    "destination": "/docs/key-features/version-control",
    "permanent": true
  },
  {
    "source": "/docs/groups/inheritance",
    "destination": "/docs/permissions-and-sharing/cascading-permissions",
    "permanent": true
  },
  {
    "source": "/docs/groups/intro",
    "destination": "/docs/permissions-and-sharing/overview",
    "permanent": true
  },
  {
    "source": "/docs/groups/quickstart",
    "destination": "/docs/permissions-and-sharing/quickstart",
    "permanent": true
  },
  {
    "source": "/docs/groups/sharing",
    "destination": "/docs/permissions-and-sharing/sharing",
    "permanent": true
  },
  {
    "source": "/docs/getting-started/quickstart",
    "destination": "/docs/quickstart",
    "permanent": true
  },
  {
    "source": "/docs/design-patterns/form",
    "destination": "/docs/reference/design-patterns/form",
    "permanent": true
  },
  {
    "source": "/docs/design-patterns/history-patterns",
    "destination": "/docs/reference/design-patterns/history-patterns",
    "permanent": true
  },
  {
    "source": "/docs/design-patterns/organization",
    "destination": "/docs/reference/design-patterns/organization",
    "permanent": true
  },
  {
    "source": "/docs/resources/encryption",
    "destination": "/docs/reference/encryption",
    "permanent": true
  },
  {
    "source": "/docs/faq",
    "destination": "/docs/reference/faq",
    "permanent": true
  },
  {
    "source": "/docs/performance",
    "destination": "/docs/reference/performance",
    "permanent": true
  },
  {
    "source": "/docs/server-side/http-requests",
    "destination": "/docs/server-side/communicating-with-workers/http-requests",
    "permanent": true
  },
  {
    "source": "/docs/server-side/inbox",
    "destination": "/docs/server-side/communicating-with-workers/inbox",
    "permanent": true
  },
  {
    "source": "/docs/server-side/communicating-with-workers",
    "destination": "/docs/server-side/communicating-with-workers/overview",
    "permanent": true
  },
  {
    "source": "/docs/project-setup/ssr",
    "destination": "/docs/server-side/ssr",
    "permanent": true
  },
  {
    "source": "/docs/ai-tools",
    "destination": "/docs/tooling-and-resources/ai-tools",
    "permanent": true
  },
  {
    "source": "/docs/tools/create-jazz-app",
    "destination": "/docs/tooling-and-resources/create-jazz-app",
    "permanent": true
  },
  {
    "source": "/docs/inspector",
    "destination": "/docs/tooling-and-resources/inspector",
    "permanent": true
  }
];
  allRedirects.push(...initialSet);
  for (const framework of frameworks) {
    const frameworkRedirects = initialSet.map(redirect => {
      return {
        source: redirect.source.replace('/docs', `/docs/${framework}`),
        destination: redirect.destination.replace('/docs', `/docs/${framework}`),
        permanent: true
      }
    })
    allRedirects.push(...frameworkRedirects);
  }
  return allRedirects;
}