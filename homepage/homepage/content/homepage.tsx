import Link from "next/link";

export const heroCopy = {
  kicker: "Reactive, distributed, secure",
  headline: "Smooth database.",
  description:
    "Jazz is a database that's distributed across your frontend, containers and functions. It syncs structured data, files and LLM streams instantly and looks like local reactive JSON state.",
  descriptionLong: (
    <>
      <p>
        Jazz is a new kind of database that's distributed across your frontend,
        containers, serverless functions and its own storage cloud.
      </p>
      <p>
        It syncs structured data, files and LLM streams instantly.
        <br />
        It looks like local reactive JSON state.
      </p>
      <p>
        And you get auth, orgs & teams, real-time multiplayer, edit histories,
        permissions, E2E encryption and offline-support out of the box.
      </p>
      <p>
        This lets you get rid of 90% of the traditional backend, and most of
        your frontend state juggling. You&apos;ll ship better apps, faster.
      </p>
      <p className="text-base">
        Self-host or use{" "}
        <Link className="text-reset" href="/cloud">
          Jazz Cloud
        </Link>{" "}
        for a zero-deploy globally-scaled DB.
        <br />
        Open source (MIT)
      </p>
    </>
  ),
};
