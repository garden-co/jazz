// @vitest-environment happy-dom

import { co, Account, CoValueLoadingState, Group } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";
import { render, screen, waitFor } from "./testUtils";
import TestCoStateWrapper from "./TestCoStateWrapper.svelte";

describe("CoState", () => {
  let serverAccount: Account;
  let clientAccount: Account;
  let publicGroup: Group;

  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      creationProps: { name: "Server" },
      isCurrentActiveAccount: true,
    });

    serverAccount = Account.getMe();
    clientAccount = await createJazzTestAccount({
      creationProps: { name: "Client" },
    });
    publicGroup = Group.create(serverAccount).makePublic("reader");
  });

  it("should use the schema's resolve query if no resolve query is provided", async () => {
    const Person = co.map({
      name: co.plainText(),
    });
    const PersonWithName = Person.resolved({ name: true });
    const person = Person.create({ name: "John Doe" }, publicGroup);

    render(
      TestCoStateWrapper,
      {
        Schema: PersonWithName,
        id: person.$jazz.id,
      },
      {
        account: clientAccount,
      },
    );

    await waitFor(() => {
      expect(screen.getByTestId("loading-state").textContent).toBe(
        CoValueLoadingState.LOADED,
      );
    });
    expect(screen.getByTestId("is-loaded").textContent).toBe("true");
    const stateValue = JSON.parse(
      screen.getByTestId("state-value").textContent ?? "",
    );
    expect(stateValue.name).toBe("John Doe");
  });

  it("should return a 'deleted' value when the coValue is deleted", async () => {
    const Person = co.map({
      name: co.plainText(),
    });
    const PersonWithName = Person.resolved({ name: true });
    const person = Person.create({ name: "John Doe" }, publicGroup);

    render(
      TestCoStateWrapper,
      {
        Schema: PersonWithName,
        id: person.$jazz.id,
      },
      {
        account: clientAccount,
      },
    );

    await waitFor(() => {
      expect(screen.getByTestId("loading-state").textContent).toBe(
        CoValueLoadingState.LOADED,
      );
    });

    // Delete on the server (owner/admin) and ensure it propagates.
    person.$jazz.raw.core.deleteCoValue();
    await person.$jazz.raw.core.waitForSync();

    await waitFor(() => {
      expect(screen.getByTestId("loading-state").textContent).toBe(
        CoValueLoadingState.DELETED,
      );
      expect(screen.getByTestId("is-loaded").textContent).toBe("false");
    });
  });
});
