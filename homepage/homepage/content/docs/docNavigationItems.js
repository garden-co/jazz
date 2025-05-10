export const docNavigationItems = [
  {
    // welcome to jazz
    name: "Getting started",
    items: [
      {
        // what is jazz, supported environments, where to start (guide, examples, project setup)
        name: "Introduction",
        href: "/docs",
        done: 100,
      },
      {
        name: "Guide",
        href: "/docs/guide",
        done: {
          react: 100,
        },
      },
      {
        name: "Example apps",
        href: "/examples",
        done: 30,
      },
      { name: "FAQs", href: "/docs/faq", done: 100 },
    ],
  },
  {
    name: "Project setup",
    items: [
      {
        name: "Installation",
        href: "/docs/project-setup",
        done: {
          react: 100,
          vue: 100,
          "react-native": 100,
          "react-native-expo": 100,
          svelte: 100,
        },
      },
      {
        // jazz mesh, setting api key, free plan, unlimited
        name: "Sync and storage",
        href: "/docs/sync-and-storage",
        done: 100,
      },
      {
        name: "Node.JS / server workers",
        href: "/docs/project-setup/server-side",
        done: 80,
      },
      {
        name: "Providers",
        href: "/docs/project-setup/providers",
        done: {
          react: 100,
          "react-native": 100,
          "react-native-expo": 100,
        },
      },
    ],
  },
  {
    name: "Tools",
    items: [
      {
        name: "AI tools",
        href: "/docs/ai-tools",
        done: 100,
      },
      {
        name: "create-jazz-app",
        href: "/docs/tools/create-jazz-app",
        done: 100,
      },
      {
        name: "Inspector",
        href: "/docs/inspector",
        done: 100,
      },
    ],
  },
  {
    name: "Upgrade guides",
    collapse: true,
    prefix: "/docs/upgrade",
    items: [
      {
        name: "0.13.0 - React Native Split",
        href: "/docs/upgrade/0-13-0",
        done: 100,
      },
      {
        // upgrade guides
        name: "0.12.0 - Deeply Resolved Data",
        href: "/docs/upgrade/0-12-0",
        done: 100,
      },
      {
        // upgrade guides
        name: "0.11.0 - Roles and permissions",
        href: "/docs/upgrade/0-11-0",
        done: 100,
      },
      // {
      //   // upgrade guides
      //   name: "0.10.0 - New authentication flow",
      //   href: "/docs/upgrade/0-10-0",
      //   done: 100,
      // },
      // {
      //   // upgrade guides
      //   name: "0.9.8 - Without me!",
      //   href: "/docs/upgrade/0-9-8",
      //   done: 100,
      // },
      // {
      //   // upgrade guides
      //   name: "0.9.2 - Local persistence on React Native",
      //   href: "/docs/upgrade/react-native-local-persistence",
      //   done: 100,
      //   framework: "react-native",
      // },
      {
        // upgrade guides
        name: "0.9.2 - Local persistence on React Native Expo",
        href: "/docs/upgrade/react-native-local-persistence",
        done: 100,
        framework: "react-native-expo",
      },
      // {
      //   // upgrade guides
      //   name: "0.9.0 - Top level imports",
      //   href: "/docs/upgrade/0-9-0",
      //   done: 100,
      // },
    ],
  },
  {
    name: "Defining schemas",
    items: [
      {
        name: "CoValues",
        href: "/docs/schemas/covalues",
        done: 20,
      },
      {
        name: "Accounts & migrations",
        href: "/docs/schemas/accounts-and-migrations",
        done: 20,
      },
    ],
  },
  {
    name: "Using CoValues",
    items: [
      {
        name: "CoMaps",
        href: "/docs/using-covalues/comaps",
        done: 100,
      },
      {
        name: "CoLists",
        href: "/docs/using-covalues/colists",
        done: 100,
      },
      {
        name: "CoFeeds",
        href: "/docs/using-covalues/cofeeds",
        done: 100,
      },
      {
        name: "CoTexts",
        href: "/docs/using-covalues/cotexts",
        done: 100,
      },
      {
        name: "FileStreams",
        href: "/docs/using-covalues/filestreams",
        done: 80,
      },
      {
        name: "ImageDefinition",
        href: "/docs/using-covalues/imagedef",
        done: {
          react: 100,
          "react-native": 100,
          "react-native-expo": 100,
          vanilla: 100,
        },
      },
      {
        name: "SchemaUnions",
        href: "/docs/using-covalues/schemaunions",
        done: 0,
      },
      {
        name: "Subscriptions & Deep Loading",
        href: "/docs/using-covalues/subscription-and-loading",
        done: 80,
      },
      {
        name: "History & time travel",
        href: "/docs/using-covalues/history",
        done: 0,
      },
    ],
  },
  {
    name: "Groups, permissions & sharing",
    items: [
      {
        name: "Groups as permission scopes",
        href: "/docs/groups/intro",
        done: 10,
      },
      {
        name: "Public sharing & invites",
        href: "/docs/groups/sharing",
        done: 10,
      },
      {
        name: "Group inheritance",
        href: "/docs/groups/inheritance",
        done: 100,
      },
    ],
  },
  {
    name: "Authentication",
    items: [
      {
        name: "Overview",
        href: "/docs/authentication/overview",
        done: 100,
      },
      {
        name: "Authentication States",
        href: "/docs/authentication/authentication-states",
        done: 100,
      },
      {
        name: "Passkey",
        href: "/docs/authentication/passkey",
        done: 100,
      },
      {
        name: "Passphrase",
        href: "/docs/authentication/passphrase",
        done: 100,
      },
      {
        name: "Clerk",
        href: "/docs/authentication/clerk",
        done: 100,
      },
      {
        name: "Cross-Device Account Transfer",
        href: "/docs/authentication/cross-device-account-transfer",
        done: 100,
      },
      {
        name: "Writing your own",
        href: "/docs/authentication/writing-your-own",
        done: 0,
      },
    ],
  },
  {
    name: "Design patterns",
    items: [
      {
        name: "Form",
        href: "/docs/design-patterns/form",
        done: 100,
      },
      {
        name: "Organization/Team",
        href: "/docs/design-patterns/organization",
        done: 80,
      },
    ],
  },
  {
    name: "Resources",
    items: [
      {
        name: "Jazz under the hood",
        href: "/docs/jazz-under-the-hood",
        done: 0,
      },
    ],
  },
];
