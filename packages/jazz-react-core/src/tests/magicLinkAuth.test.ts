// @vitest-environment happy-dom

import { describe, expect, it, beforeEach } from "vitest";
import {
  useCreateMagicLinkAuthAsProvider,
  useCreateMagicLinkAuthAsConsumer,
  useHandleMagicLinkAuthAsConsumer,
  useHandleMagicLinkAuthAsProvider,
} from "../auth/MagicLinkAuth.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { renderHook, act, waitFor } from "./testUtils";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("MagicLinkAuth", () => {
  describe("useCreateMagicLinkAuthAsProvider", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("throws error when using guest account", async () => {
      const guestAccount = await createJazzTestGuest();

      expect(() =>
        renderHook(
          () => useCreateMagicLinkAuthAsProvider(window.location.origin),
          { account: guestAccount },
        ),
      ).toThrowError("Magic Link Auth is not supported in guest mode");
    });

    it("initializes with idle state", async () => {
      const account = await createJazzTestAccount({});

      const { result: createAsProvider } = renderHook(
        () => useCreateMagicLinkAuthAsProvider(window.location.origin),
        { account },
      );

      expect(createAsProvider.current.status).toBe("idle");
      expect(createAsProvider.current.createLink).toBeTypeOf("function");
      expect(createAsProvider.current.confirmationCode).toBeUndefined();
      createAsProvider.current.cancelFlow();
    });

    it("can create a magic link and cancel flow", async () => {
      const account = await createJazzTestAccount({});
      const { result: createAsProvider } = renderHook(
        () => useCreateMagicLinkAuthAsProvider(window.location.origin),
        { account },
      );
      let link = "";
      await act(async () => {
        link = await createAsProvider.current.createLink();
      });
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/magic-link-handler-consumer\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsProvider.current.status).toBe("waitingForConsumer");
      act(() => {
        createAsProvider.current.cancelFlow();
      });
      await waitFor(() => {
        expect(createAsProvider.current.status).toBe("cancelled");
      });
    });
  });

  describe("useCreateMagicLinkAuthAsConsumer", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("throws error when using guest account", async () => {
      const guestAccount = await createJazzTestGuest();
      expect(() =>
        renderHook(
          () => useCreateMagicLinkAuthAsConsumer(window.location.origin),
          { account: guestAccount },
        ),
      ).toThrowError("Magic Link Auth is not supported in guest mode");
    });

    it("initializes with idle state", async () => {
      const { result: createAsConsumer } = renderHook(() =>
        useCreateMagicLinkAuthAsConsumer(window.location.origin),
      );
      expect(createAsConsumer.current.status).toBe("idle");
      expect(createAsConsumer.current.createLink).toBeTypeOf("function");
      expect(createAsConsumer.current.sendConfirmationCode).toBeUndefined();
      createAsConsumer.current.cancelFlow();
    });

    it("can create a magic link and cancel flow", async () => {
      const account = await createJazzTestAccount({});
      const { result: createAsConsumer } = renderHook(
        () => useCreateMagicLinkAuthAsConsumer(window.location.origin),
        { account },
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/magic-link-handler-provider\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsConsumer.current.status).toBe("waitingForProvider");
      act(() => {
        createAsConsumer.current.cancelFlow();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("cancelled");
      });
    });
  });

  describe("useHandleMagicLinkAuthAsProvider", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("initializes with idle state", async () => {
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () =>
          useHandleMagicLinkAuthAsProvider(
            window.location.origin,
            "invalid-link-gets-ignored",
          ),
        { account },
      );
      expect(handleAsProvider.current.status).toBe("idle");
      expect(handleAsProvider.current.confirmationCode).toBeUndefined();
    });

    it("handles the flow", async () => {
      // Create consumer
      const { result: createAsConsumer } = renderHook(() =>
        useCreateMagicLinkAuthAsConsumer(window.location.origin),
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("waitingForProvider");
      });

      // Create provider
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () => useHandleMagicLinkAuthAsProvider(window.location.origin, link),
        { account },
      );

      // Confirmation code should be generated by the provider, and the consumer should be waiting for it
      await waitFor(() => {
        expect(handleAsProvider.current.status).toBe(
          "confirmationCodeGenerated",
        );
      });
      expect(handleAsProvider.current.confirmationCode).toBeDefined();
      expect(createAsConsumer.current.status).toBe("confirmationCodeRequired");
      expect(createAsConsumer.current.sendConfirmationCode).toBeDefined();

      // The consumer should send the confirmation code to the provider
      await act(async () => {
        createAsConsumer.current.sendConfirmationCode!(
          handleAsProvider.current.confirmationCode!,
        );
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("authorized");
      });
      expect(handleAsProvider.current.status).toBe("authorized");
    });
  });

  describe("useHandleMagicLinkAuthAsConsumer", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("initializes with idle state", async () => {
      const { result: handleAsConsumer } = renderHook(() =>
        useHandleMagicLinkAuthAsConsumer(
          window.location.origin,
          "invalid-link-gets-ignored",
        ),
      );
      expect(handleAsConsumer.current.status).toBe("idle");
      expect(handleAsConsumer.current.sendConfirmationCode).toBeNull();
    });

    it("handles the flow", async () => {
      // Create consumer
      const { result: createAsConsumer } = renderHook(() =>
        useCreateMagicLinkAuthAsConsumer(window.location.origin),
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("waitingForProvider");
      });

      // Create provider
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () => useHandleMagicLinkAuthAsProvider(window.location.origin, link),
        { account },
      );

      // Confirmation code should be generated by the provider, and the consumer should be waiting for it
      await waitFor(() => {
        expect(handleAsProvider.current.status).toBe(
          "confirmationCodeGenerated",
        );
      });
      expect(handleAsProvider.current.confirmationCode).toBeDefined();
      expect(createAsConsumer.current.status).toBe("confirmationCodeRequired");
      expect(createAsConsumer.current.sendConfirmationCode).toBeDefined();

      // The consumer should send the confirmation code to the provider
      await act(async () => {
        createAsConsumer.current.sendConfirmationCode!(
          handleAsProvider.current.confirmationCode!,
        );
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("authorized");
      });
      expect(handleAsProvider.current.status).toBe("authorized");
    });
  });
});
