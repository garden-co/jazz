/** @type {@import('./docNavigationItemsTypes').DocNavigationSection[]} */
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
        excludeFromNavigation: true,
      }, {
        name: "Quickstart",
        href: "/docs/getting-started/quickstart",
        done: 100,
      },
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
        name: "Troubleshooting",
        href: "/docs/troubleshooting",
        done: 100,
      },
    ],
  },
  {
    name: "Core Concepts",
    href: '/docs',
    items: [
      {
        name: "CoValues",
        href: '/docs/schemas/covalues',
        done: 100,
        collapse: true,
        startClosed: true,
        items: [{
          name: "CoMaps",
          href: "/docs/using-covalues/comaps",
          done: 100,
        }, {
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
            svelte: 100,
          }
        }],
      }, {
        name: 'Defining Schemas',
        done: 100,
        collapse: true,
        items: [{
          name: "Connecting CoValues",
          href: "/docs/using-covalues/connecting-covalues",
          done: 100,
        },
        {
          name: "Accounts & migrations",
          href: "/docs/schemas/accounts-and-migrations",
          done: 20,
        },
        {
          name: "Schema Unions",
          href: "/docs/using-covalues/schemaunions",
          done: 100,
        }]
      }, {
        // jazz mesh, setting api key, free plan, unlimited
        name: "Sync and storage",
        href: "/docs/sync-and-storage",
        done: 100,
      },
    ],
  },
  {
    name: "Key Features",
    done: 100,
    items: [{
      name: 'Authentication',
      startClosed: true,
      collapse: true,
      items: [
        {
          name: "Quickstart",
          href: "/docs/authentication/quickstart",
          done: 100,
        },
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
        // {
        //   name: "Jazz Cloud",
        //   href: "/docs/authentication/jazz-cloud",
        //   done: {
        //     react: 100,
        //     vanilla: 100,
        //     "react-native-expo": 100,
        //   },
        // },
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
          name: "Better Auth",
          href: "/docs/authentication/better-auth",
          done: 100,
        },
        //{
        //  name: "Writing your own",
        //  href: "/docs/authentication/writing-your-own",
        //  done: 0,
        //},
      ]
    }, {
      name: "Permissions & sharing",
      collapse: true,
      startClosed: true,
      items: [
        {
          name: "Groups",
          href: "/docs/groups/intro",
          done: 10,
        },
        {
          name: "Sharing",
          href: "/docs/groups/sharing",
          done: 10,
        },
        {
          name: "Cascading Permissions",
          href: "/docs/groups/inheritance",
          done: 100,
        },
      ],
    }, {
      name: "Data Flow",
      collapse: true,
      startClosed: true,
      items: [
        {
          name: "Subscriptions & Deep Loading",
          href: "/docs/using-covalues/subscription-and-loading",
          done: 80,
        },
        {
          name: "Version control",
          href: "/docs/using-covalues/version-control",
          done: 100,
        },
        {
          name: "History",
          href: "/docs/using-covalues/history",
          done: 100,
        },
      ],
    }]
  },
  {
    name: "Server-Side Development",
    items: [{
      name: "Quickstart",
      href: "/docs/server-side/quickstart",
      done: 100,
    },
    {
      name: "Setup",
      href: "/docs/server-side/setup",
      done: 100,
    },
    {
      name: "Communicating with workers",
      href: "/docs/server-side/communicating-with-workers",
      done: 100,
      startClosed: true,
      collapse: true,
      items: [{
        name: "HTTP requests",
        href: "/docs/server-side/http-requests",
        done: 100,
      },
      {
        name: "Inbox API",
        href: "/docs/server-side/inbox",
        done: 100,
      }]
    }, {
      name: "Server-side rendering",
      href: "/docs/project-setup/ssr",
      done: {
        react: 100,
      }
    },
    ],
  },
  {
    name: "Project setup",
    items: [
      {
        name: "Providers",
        href: "/docs/project-setup/providers",
        done: {
          react: 100,
          "react-native": 100,
          "react-native-expo": 100,
          svelte: 100,
        },
      },

    ],
  },
  {
    name: "Upgrade guides",
    collapse: true,
    prefix: "/docs/upgrade",
    items: [
      {
        name: "0.18.0 - New `$jazz` field in CoValues",
        href: "/docs/upgrade/0-18-0",
        done: 100,
      },
      {
        name: "0.17.0 - New image APIs",
        href: "/docs/upgrade/0-17-0",
        done: 100,
      },
      {
        name: "0.16.0 - Cleaner separation between Zod and CoValue schemas",
        href: "/docs/upgrade/0-16-0",
        done: 100,
      },
      {
        name: "0.15.0 - Everything inside `jazz-tools`",
        href: "/docs/upgrade/0-15-0",
        done: 100,
      },
      {
        name: "0.14.0 - Zod-based schemas",
        href: "/docs/upgrade/0-14-0",
        done: 100,
      },
      // {
      //   name: "0.13.0 - React Native Split",
      //   href: "/docs/upgrade/0-13-0",
      //   done: 100,
      //   excludeFromNavigation: true,
      // },
      // {
      //   // upgrade guides
      //   name: "0.12.0 - Deeply Resolved Data",
      //   href: "/docs/upgrade/0-12-0",
      //   done: 100,
      //   excludeFromNavigation: true,
      // },
      // {
      //   // upgrade guides
      //   name: "0.11.0 - Roles and permissions",
      //   href: "/docs/upgrade/0-11-0",
      //   done: 100,
      //   excludeFromNavigation: true,
      // },
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
      // {
      //   // upgrade guides
      //   name: "0.9.2 - Local persistence on React Native Expo",
      //   href: "/docs/upgrade/react-native-local-persistence",
      //   done: 100,
      //   framework: "react-native-expo",
      //   excludeFromNavigation: true,
      // },
      // {
      //   // upgrade guides
      //   name: "0.9.0 - Top level imports",
      //   href: "/docs/upgrade/0-9-0",
      //   done: 100,
      // },
    ],
  }

  ,

  {
    name: "Tooling & Resources",
    items: [
      {
        name: "Developer Tools",
        items: [{
          name: "create-jazz-app",
          href: "/docs/tools/create-jazz-app",
          done: 100,
        },
        {
          name: "Inspector",
          href: "/docs/inspector",
          done: 100,
        }, {
          name: "AI tools",
          href: "/docs/ai-tools",
          done: 100,
        }]
      }, {
        name: "Reference",
        items: [{
          name: "Encryption",
          href: "/docs/resources/encryption",
          done: 100,
        }, { name: "FAQs", href: "/docs/faq", done: 100 }, {
          name: "Design patterns",
          collapse: true,
          items: [
            {
              name: "Forms",
              href: "/docs/design-patterns/form",
              done: 100,
            },
            {
              name: "Organization/Team",
              href: "/docs/design-patterns/organization",
              done: 80,
            },
            {
              name: "History Patterns",
              href: "/docs/design-patterns/history-patterns",
              done: 100,
            },
          ],
        }]
      }
    ],
  },
];

const flatItems = docNavigationItems
  .flatMap((section) => section.items)
  .filter((item) => !item.excludeFromNavigation);

export const flatItemsWithNavLinks = flatItems.map((item, index) => {
  const findNextWithHref = (startIndex) => {
    for (let i = startIndex + 1; i < flatItems.length; i++) {
      if (flatItems[i].href) return flatItems[i];
    }
    return null;
  };

  const findPreviousWithHref = (startIndex) => {
    for (let i = startIndex - 1; i >= 0; i--) {
      if (flatItems[i].href) return flatItems[i];
    }
    return null;
  };

  return {
    ...item,
    next: item.next === null ? null : findNextWithHref(index),
    previous: item.previous === null ? null : findPreviousWithHref(index),
  };
});
