// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { co, z, CoValueLoadingState } from "jazz-tools";
import { describe, bench } from "vitest";
import {
  useAccountSubscription,
  useSubscriptionSelector,
  useAccountWithSelector,
  CoValueSubscription,
} from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { render } from "./testUtils.js";

cojsonInternals.setCoValueLoadingRetryDelay(300);

await setupJazzTestSync();
await createJazzTestAccount({
  isCurrentActiveAccount: true,
  creationProps: { name: "Hermes Puggington" },
});

const AccountSchema = co.account();

const AccountName = () => {
  const name = useAccountWithSelector(AccountSchema, {
    resolve: {
      profile: true,
    },
    select: (account) => {
      if (account.$jazzState !== CoValueLoadingState.LOADED) {
        return null;
      }
      return account.profile.name;
    },
  });

  if (!name) return null;

  return <span>{name}</span>;
};

const AccountNameFromSubscription = ({
  subscription,
}: {
  subscription: CoValueSubscription<typeof AccountSchema, { profile: true }>;
}) => {
  const name = useSubscriptionSelector(subscription, {
    select: (account) => {
      if (account.$jazzState !== CoValueLoadingState.LOADED) {
        return null;
      }
      return account.profile.name;
    },
  });

  if (!name) return null;

  return <span>{name}</span>;
};

const UseAccountWithSelector = () => {
  return (
    <div>
      {Array.from({ length: 1000 }).map((_, i) => (
        <AccountName key={i} />
      ))}
    </div>
  );
};

const UseAccountSubscription = () => {
  const subscription = useAccountSubscription(AccountSchema, {
    resolve: {
      profile: true,
    },
  });

  return (
    <div>
      {Array.from({ length: 1000 }).map((_, i) => (
        <AccountNameFromSubscription key={i} subscription={subscription} />
      ))}
    </div>
  );
};

describe("1000 value loads", () => {
  bench(
    "useAccountSubscription + useSubscriptionSelector",
    () => {
      render(<UseAccountSubscription />);
    },
    { iterations: 50 },
  );

  bench(
    "useAccountWithSelector",
    () => {
      render(<UseAccountWithSelector />);
    },
    { iterations: 50 },
  );
});

describe("deeply resolved coMaps", async () => {
  const Task = co.map({
    title: z.string(),
    get project() {
      return Project;
    },
  });

  const TaskList = co.list(Task);

  const Project = co.map({
    name: z.string(),
    tasks: TaskList,
    draftTasks: TaskList,
    deletedTasks: TaskList,
  });

  const Organization = co.map({
    name: z.string(),
    projects: co.list(Project),
  });

  const AccountRoot = co.map({
    organizations: co.list(Organization),
  });

  const AccountSchema = co
    .account({
      root: AccountRoot,
      profile: co.profile(),
    })
    .withMigration(async (account) => {
      if (!account.$jazz.has("root")) {
        account.$jazz.set("root", {
          organizations: [
            {
              name: "My organization",
              projects: [
                {
                  name: "My project",
                  tasks: [],
                  draftTasks: [],
                  deletedTasks: [],
                },
              ],
            },
          ],
        });
      }
    });

  const account = await createJazzTestAccount({
    AccountSchema,
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });

  const root = await account.root.$jazz.ensureLoaded({
    resolve: {
      organizations: {
        $each: {
          projects: {
            $each: true,
          },
        },
      },
    },
  });

  const project = root.organizations[0]?.projects[0]!;

  for (let i = 0; i < 250; i++) {
    const taskList = (["tasks", "draftTasks", "deletedTasks"] as const)[i % 3]!;

    project[taskList].$jazz.push({
      title: `Task ${i}`,
      project,
    });
  }

  const SingleSubscriptionTasks = ({
    subscription,
    taskListType,
  }: {
    subscription: CoValueSubscription<
      typeof AccountSchema,
      {
        root: {
          organizations: {
            $each: {
              projects: {
                $each: {
                  tasks: { $each: { project: true } };
                  draftTasks: { $each: { project: true } };
                  deletedTasks: { $each: { project: true } };
                };
              };
            };
          };
        };
      }
    >;
    taskListType: "tasks" | "draftTasks" | "deletedTasks";
  }) => {
    const allProjectsTasks = useSubscriptionSelector(subscription, {
      select: (account) => {
        if (account.$jazzState !== CoValueLoadingState.LOADED) {
          return null;
        }
        return account.root.organizations.flatMap((org) =>
          org.projects.flatMap((project) =>
            project[taskListType].flatMap((task) => task),
          ),
        );
      },
    });

    return (
      <div>
        {allProjectsTasks?.map((task) => (
          <div key={task.$jazz.id}>{task.title}</div>
        ))}
      </div>
    );
  };

  const SingleSubscription = () => {
    const subscription = useAccountSubscription(AccountSchema, {
      resolve: {
        root: {
          organizations: {
            $each: {
              projects: {
                $each: {
                  tasks: { $each: { project: true } },
                  draftTasks: { $each: { project: true } },
                  deletedTasks: { $each: { project: true } },
                },
              },
            },
          },
        },
      },
    });

    return (
      <div>
        <SingleSubscriptionTasks
          subscription={subscription}
          taskListType="tasks"
        />
        <SingleSubscriptionTasks
          subscription={subscription}
          taskListType="draftTasks"
        />
        <SingleSubscriptionTasks
          subscription={subscription}
          taskListType="deletedTasks"
        />
      </div>
    );
  };

  bench(
    "useAccountSubscription + useSubscriptionSelector",
    () => {
      render(<SingleSubscription />);
    },
    { iterations: 200 },
  );

  const MultipleSubscriptionTasks = ({
    taskListType,
  }: {
    taskListType: "tasks" | "draftTasks" | "deletedTasks";
  }) => {
    const subscription = useAccountWithSelector(AccountSchema, {
      resolve: {
        root: {
          organizations: {
            $each: {
              projects: {
                $each: {
                  tasks: { $each: { project: true } },
                  draftTasks: { $each: { project: true } },
                  deletedTasks: { $each: { project: true } },
                },
              },
            },
          },
        },
      },
      select: (account) => {
        if (account.$jazzState !== CoValueLoadingState.LOADED) {
          return null;
        }
        return account.root.organizations.flatMap((org) =>
          org.projects.flatMap((project) =>
            project[taskListType].flatMap((task) => task),
          ),
        );
      },
    });

    return (
      <div>
        {subscription?.map((task) => (
          <div key={task.$jazz.id}>{task.title}</div>
        ))}
      </div>
    );
  };

  const MultipleSubscription = () => {
    return (
      <div>
        <MultipleSubscriptionTasks taskListType="tasks" />
        <MultipleSubscriptionTasks taskListType="draftTasks" />
        <MultipleSubscriptionTasks taskListType="deletedTasks" />
      </div>
    );
  };

  bench(
    "useAccountWithSelector",
    () => {
      render(<MultipleSubscription />);
    },
    { iterations: 200 },
  );
});
