/** @type {@import('./docNavigationItemsTypes').DocNavigationSection[]} */
export const docNavigationItems = [
	{
		// welcome to jazz
		name: "Getting started",
		href: "/docs",
		items: [
			{
				// what is Jazz, supported environments, where to start (guide, examples, project setup)
				name: "Overview",
				href: "/docs",
				done: 100,
			},
			{
				name: "Quickstart",
				href: "/docs/quickstart",
				done: {
					react: 100,
					svelte: 100,
					vanilla: 100
				}
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
					vanilla: 100,
				},
			},
			{
				name: "API Reference",
				href: "/docs/api-reference",
				done: 100,
			},
			{
				name: "Troubleshooting",
				href: "/docs/troubleshooting",
				done: 100,
			},
		],
	},
	{
		name: "Upgrade guides",
		collapse: true,
		startClosed: true,
		prefix: "/docs/upgrade",
		items: [
			{
				name: "0.20.0 - Full native crypto",
				href: "/docs/upgrade/0-20-0",
				done: 100,
			},
			{
				name: "0.19.0 - Explicit loading states",
				href: "/docs/upgrade/0-19-0",
				done: 100,
			},
			{
				name: "0.18.0 - New `$jazz` field",
				href: "/docs/upgrade/0-18-0",
				done: 100,
			},
			{
				name: "0.17.0 - New image APIs",
				href: "/docs/upgrade/0-17-0",
				done: 100,
			},
			// {
			//  name: "0.16.0 - Cleaner separation between Zod and CoValue schemas",
			//  href: "/docs/upgrade/0-16-0",
			//  done: 100,
			// },
			// {
			//  name: "0.15.0 - Everything inside `jazz-tools`",
			//  href: "/docs/upgrade/0-15-0",
			//  done: 100,
			// },
			// {
			//  name: "0.14.0 - Zod-based schemas",
			//  href: "/docs/upgrade/0-14-0",
			//  done: 100,
			// },
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
			//   name: "0.10.0 - New key-features/authentication flow",
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
	},
	{
		name: "Core Concepts",
		done: 100,
		prefix: "/docs/core-concepts",
		items: [
			{
				name: "CoValues",
				done: 100,
				collapse: true,
				startClosed: true,
				items: [
					{
						name: "Overview",
						href: "/docs/core-concepts/covalues/overview",
						done: 100,
					},
					{
						name: "CoMaps",
						href: "/docs/core-concepts/covalues/comaps",
						done: 100,
					},
					{
						name: "CoLists",
						href: "/docs/core-concepts/covalues/colists",
						done: 100,
					},
					{
						name: "CoFeeds",
						href: "/docs/core-concepts/covalues/cofeeds",
						done: 100,
					},
					{
						name: "CoTexts",
						href: "/docs/core-concepts/covalues/cotexts",
						done: 100,
					},
					{
						name: "FileStreams",
						href: "/docs/core-concepts/covalues/filestreams",
						done: 80,
					},
					{
						name: "CoVectors",
						href: "/docs/core-concepts/covalues/covectors",
						done: 100,
					},
					{
						name: "ImageDefinitions",
						href: "/docs/core-concepts/covalues/imagedef",
						done: {
							react: 100,
							"react-native": 100,
							"react-native-expo": 100,
							vanilla: 100,
							svelte: 100,
						},
					},
				],
			},
			{
				name: "Schemas",
				done: 100,
				collapse: true,
				items: [
					{
						name: "Connecting CoValues",
						href: "/docs/core-concepts/schemas/connecting-covalues",
						done: 100,
					},
					{
						name: "Accounts & migrations",
						href: "/docs/core-concepts/schemas/accounts-and-migrations",
						done: 20,
					},
					{
						name: "Schema Unions",
						href: "/docs/core-concepts/schemas/schemaunions",
						done: 100,
					},
					{
						name: "Codecs",
						href: "/docs/core-concepts/schemas/codecs",
						done: 100,
					},
				],
			},
			{
				name: "Subscriptions & Deep Loading",
				href: "/docs/core-concepts/subscription-and-loading",
				done: 100,
			},
			{
				// jazz mesh, setting api key, free plan, unlimited
				name: "Sync and storage",
				href: "/docs/core-concepts/sync-and-storage",
				done: 100,
			},
			{
				name: "Deleting CoValues",
				href: "/docs/core-concepts/deleting",
				done: 100,
			},
		],
	},
	{
		name: "Key Features",
		done: 100,
		prefix: "/docs/key-features",
		items: [
			{
				name: "Authentication",
				startClosed: true,
				collapse: true,
				items: [
					{
						name: "Overview",
						href: "/docs/key-features/authentication/overview",
						done: 100,
					},
					{
						name: "Quickstart",
						href: "/docs/key-features/authentication/quickstart",
						done: {
							react: 100,
							svelte: 100,
							vanilla: 100
						},
					},
					{
						name: "Authentication States",
						href: "/docs/key-features/authentication/authentication-states",
						done: 100,
					},
					{
						name: "Passkey",
						href: "/docs/key-features/authentication/passkey",
						done: {
							react: 100,
							svelte: 100,
							vanilla: 100,
						},
					},
					{
						name: "Passphrase",
						href: "/docs/key-features/authentication/passphrase",
						done: 100,
					},
					{
						name: "Clerk",
						href: "/docs/key-features/authentication/clerk",
						done: 100,
					},
					{
						name: "Better Auth",
						href: "/docs/key-features/authentication/better-auth",
						done: 100,
					},
					{
						name: "Better Auth Database Adapter",
						href: "/docs/key-features/authentication/better-auth-database-adapter",
						done: 100,
					},
				],
			},
			{
				name: "Permissions & sharing",
				collapse: true,
				startClosed: true,
				items: [
					{
						name: "Overview",
						href: "/docs/permissions-and-sharing/overview",
						done: 100,
					},
					{
						name: "Quickstart",
						href: "/docs/permissions-and-sharing/quickstart",
						done: {
							react: 100,
							svelte: 100,
							vanilla: 100,
						},
					},
					{
						name: "Sharing",
						href: "/docs/permissions-and-sharing/sharing",
						done: 100,
					},
					{
						name: "Cascading Permissions",
						href: "/docs/permissions-and-sharing/cascading-permissions",
						done: 100,
					},
				],
			},
			{
				name: "Version control",
				href: "/docs/key-features/version-control",
				done: {
					react: 100,
				},
			},
			{
				name: "History",
				href: "/docs/key-features/history",
				done: 100,
			},
		],
	},
	{
		name: "Server-Side Development",
		prefix: "/docs/server-side",
		items: [
			{
				name: "Quickstart",
				href: "/docs/server-side/quickstart",
				done: {
					react: 100,
					svelte: 100,
				},
			},
			{
				name: "Setup",
				href: "/docs/server-side/setup",
				done: 100,
			},
			{
				name: "Communicating with workers",
				done: 100,
				prefix: "/docs/server-side/communicating-with-workers",
				startClosed: true,
				collapse: true,
				items: [
					{
						name: "Overview",
						href: "/docs/server-side/communicating-with-workers/overview",
						done: 100,
					},
					{
						name: "JazzRPC",
						href: "/docs/server-side/jazz-rpc",
						done: 100,
					},
					{
						name: "HTTP requests",
						href: "/docs/server-side/communicating-with-workers/http-requests",
						done: 100,
					},
					{
						name: "Inbox API",
						href: "/docs/server-side/communicating-with-workers/inbox",
						done: {
							react: 100,
						},
					},
				],
			},
			{
				name: "Server-side rendering",
				href: "/docs/server-side/ssr",
				done: {
					react: 100,
					svelte: 100,
				},
			},
		],
	},
	{
		name: "Project setup",
		prefix: "/docs/project-setup",
		items: [
			{
				name: "Providers",
				href: "/docs/project-setup/providers",
				done: 100,
			},
		],
	},
	{
		name: "Tooling & Resources",
		prefix: "/docs/tooling-and-resources",
		items: [
			{
				name: "Developer Tools",
				items: [
					{
						name: "create-jazz-app",
						href: "/docs/tooling-and-resources/create-jazz-app",
						done: 100,
					},
					{
						name: "Inspector",
						href: "/docs/tooling-and-resources/inspector",
						done: 100,
					},
					{
						name: "AI tools (llms.txt)",
						href: "/docs/tooling-and-resources/ai-tools",
						done: 100,
					},
				],
			},
			{
				name: "Reference",
				prefix: "/docs/reference",
				items: [
					{
						name: "FAQs",
						href: "/docs/reference/faq",
						done: 100,
					},
					{
						name: "Data Modelling",
						href: "/docs/reference/data-modelling",
						done: 100,
					},
					{
						name: "Encryption",
						href: "/docs/reference/encryption",
						done: 100,
					},
					{
						name: "Testing",
						href: "/docs/reference/testing",
						done: 100,
					},
					{
						name: "Performance tips",
						href: "/docs/reference/performance",
						done: 100,
					},
					{
						name: "Design patterns",
						collapse: true,
						prefix: "/docs/reference/design-patterns",
						items: [
							{
								name: "Forms",
								href: "/docs/reference/design-patterns/form",
								done: {
									react: 100,
								},
							},
							{
								name: "Organization/Team",
								href: "/docs/reference/design-patterns/organization",
								done: 100,
							},
							{
								name: "History Patterns",
								href: "/docs/reference/design-patterns/history-patterns",
								done: 100,
							},
						],
					},
				],
			},
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
