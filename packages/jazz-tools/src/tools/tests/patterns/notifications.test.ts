import { describe, expect, test, vi } from "vitest";
import { InstanceOfSchema, Loaded, co, z } from "../../exports";
import { createJazzTestAccount } from "../../testing";

const QueuedNotification = co.map({
  content: z.string(),
  sent: z.boolean(),
});

const WorkerRoot = co.map({
  notificationQueue: co.list(QueuedNotification),
});

const WorkerAccount = co
  .account({
    root: WorkerRoot,
    profile: co.profile(),
  })
  .withMigration((account) => {
    if (account.root === undefined) {
      account.$jazz.set("root", {
        notificationQueue: [],
      });
    }
  });

async function pushNotification(
  worker: InstanceOfSchema<typeof WorkerAccount>,
  content: string,
) {
  const workerAccount = await worker.$jazz.ensureLoaded({
    resolve: {
      root: {
        notificationQueue: {
          $each: true,
        },
      },
    },
  });

  const notification = QueuedNotification.create({
    content,
    sent: false,
  });

  workerAccount.root.notificationQueue.$jazz.push(notification);

  return notification;
}

async function sendAllThePendingNotifications(
  worker: InstanceOfSchema<typeof WorkerAccount>,
  sender: (notification: Loaded<typeof QueuedNotification>) => Promise<void>,
) {
  const workerAccount = await worker.$jazz.ensureLoaded({
    resolve: {
      root: {
        notificationQueue: {
          $each: true,
        },
      },
    },
  });

  for (const notification of workerAccount.root.notificationQueue) {
    if (!notification.sent) {
      notification.$jazz.set("sent", true);
      await sender(notification);
    }
  }
}

describe("Notifications", () => {
  test("when pushing a notification, it should be added to the queue", async () => {
    const worker = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      AccountSchema: WorkerAccount,
    });

    const notification = await pushNotification(worker, "Hello");

    const spy = vi.fn();

    await sendAllThePendingNotifications(worker, spy);

    expect(notification.sent).toBe(true);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});
