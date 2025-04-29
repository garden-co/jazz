// @vitest-environment happy-dom

import { beforeEach, describe, expect, it } from "vitest";
import {
  useCreateCrossDeviceAccountTransferAsSource,
  useCreateCrossDeviceAccountTransferAsTarget,
  useHandleCrossDeviceAccountTransferAsSource,
  useHandleCrossDeviceAccountTransferAsTarget,
} from "../auth/CrossDeviceAccountTransfer.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing";
import { act, renderHook, waitFor } from "./testUtils";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("CrossDeviceAccountTransfer", () => {
  describe("useCreateCrossDeviceAccountTransferAsProvider", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("throws error when using guest account", async () => {
      const guestAccount = await createJazzTestGuest();

      expect(() =>
        renderHook(
          () =>
            useCreateCrossDeviceAccountTransferAsSource(window.location.origin),
          { account: guestAccount },
        ),
      ).toThrowError(
        "Cross-Device Account Transfer is not supported in guest mode",
      );
    });

    it("initializes with idle state", async () => {
      const account = await createJazzTestAccount({});

      const { result: createAsProvider } = renderHook(
        () =>
          useCreateCrossDeviceAccountTransferAsSource(window.location.origin),
        { account },
      );

      expect(createAsProvider.current.status).toBe("idle");
      expect(createAsProvider.current.createLink).toBeTypeOf("function");
      expect(createAsProvider.current.confirmationCode).toBeUndefined();
      createAsProvider.current.cancelFlow();
    });

    it("can create a link and cancel flow", async () => {
      const account = await createJazzTestAccount({});
      const { result: createAsProvider } = renderHook(
        () =>
          useCreateCrossDeviceAccountTransferAsSource(window.location.origin),
        { account },
      );
      let link = "";
      await act(async () => {
        link = await createAsProvider.current.createLink();
      });
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/account-transfer-handler-target\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsProvider.current.status).toBe("waitingForHandler");
      act(() => {
        createAsProvider.current.cancelFlow();
      });
      await waitFor(() => {
        expect(createAsProvider.current.status).toBe("cancelled");
      });
    });
  });

  describe("useCreateCrossDeviceAccountTransferAsConsumer", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("throws error when using guest account", async () => {
      const guestAccount = await createJazzTestGuest();
      expect(() =>
        renderHook(
          () =>
            useCreateCrossDeviceAccountTransferAsTarget(window.location.origin),
          { account: guestAccount },
        ),
      ).toThrowError(
        "Cross-Device Account Transfer is not supported in guest mode",
      );
    });

    it("initializes with idle state", async () => {
      const { result: createAsConsumer } = renderHook(() =>
        useCreateCrossDeviceAccountTransferAsTarget(window.location.origin),
      );
      expect(createAsConsumer.current.status).toBe("idle");
      expect(createAsConsumer.current.createLink).toBeTypeOf("function");
      expect(createAsConsumer.current.sendConfirmationCode).toBeUndefined();
      createAsConsumer.current.cancelFlow();
    });

    it("can create a link and cancel flow", async () => {
      const account = await createJazzTestAccount({});
      const { result: createAsConsumer } = renderHook(
        () =>
          useCreateCrossDeviceAccountTransferAsTarget(window.location.origin),
        { account },
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/account-transfer-handler-source\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsConsumer.current.status).toBe("waitingForHandler");
      act(() => {
        createAsConsumer.current.cancelFlow();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("cancelled");
      });
    });
  });

  describe("useHandleCrossDeviceAccountTransferAsProvider", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("initializes with idle state", async () => {
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () =>
          useHandleCrossDeviceAccountTransferAsSource(
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
        useCreateCrossDeviceAccountTransferAsTarget(window.location.origin),
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("waitingForHandler");
      });

      // Create provider
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () =>
          useHandleCrossDeviceAccountTransferAsSource(
            window.location.origin,
            link,
          ),
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

  describe("useHandleCrossDeviceAccountTransferAsConsumer", () => {
    beforeEach(async () => {
      await createJazzTestAccount({ isCurrentActiveAccount: true });
    });

    it("initializes with idle state", async () => {
      const { result: handleAsConsumer } = renderHook(() =>
        useHandleCrossDeviceAccountTransferAsTarget(
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
        useCreateCrossDeviceAccountTransferAsTarget(window.location.origin),
      );
      let link = "";
      await act(async () => {
        link = await createAsConsumer.current.createLink();
      });
      await waitFor(() => {
        expect(createAsConsumer.current.status).toBe("waitingForHandler");
      });

      // Create provider
      const account = await createJazzTestAccount({});
      const { result: handleAsProvider } = renderHook(
        () =>
          useHandleCrossDeviceAccountTransferAsSource(
            window.location.origin,
            link,
          ),
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
