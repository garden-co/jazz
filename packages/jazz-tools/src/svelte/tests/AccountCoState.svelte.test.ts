// @vitest-environment happy-dom

import { CoValueLoadingState, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";
import { render, screen, waitFor } from "./testUtils";
import TestAccountCoStateWrapper from "./TestAccountCoStateWrapper.svelte";

describe("AccountCoState", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
  });

  it("should return a 'deleted' value when a required resolved child is deleted", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const AccountSchema = co
      .account({
        profile: co.profile(),
        root: AccountRoot,
      })
      .withMigration((account) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { value: "123" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
      isCurrentActiveAccount: true,
    });

    render(
      TestAccountCoStateWrapper,
      {
        Schema: AccountSchema,
        options: {
          resolve: {
            root: true,
          },
        },
      },
      {
        account,
      },
    );

    // Ensure the account (and root) is loaded first.
    const loaded = await account.$jazz.ensureLoaded({
      resolve: {
        root: true,
      },
    });
    assertLoaded(loaded.root);

    await waitFor(() => {
      expect(screen.getByTestId("loading-state").textContent).toBe(
        CoValueLoadingState.LOADED,
      );
      expect(screen.getByTestId("is-loaded").textContent).toBe("true");
    });

    // Delete the required child (root) -> AccountCoState should bubble the error.
    loaded.root.$jazz.raw.core.deleteCoValue();
    await loaded.root.$jazz.raw.core.waitForSync();

    await waitFor(() => {
      expect(screen.getByTestId("loading-state").textContent).toBe(
        CoValueLoadingState.DELETED,
      );
      expect(screen.getByTestId("is-loaded").textContent).toBe("false");
    });
  });
});


