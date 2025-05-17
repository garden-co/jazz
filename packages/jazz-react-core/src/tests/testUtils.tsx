import {
  RenderHookOptions,
  RenderOptions,
  render,
  renderHook,
} from "@testing-library/react";
import { Account, AnonymousJazzAgent, AuthSecretStorage } from "jazz-tools";
import React from "react";
import { JazzTestProvider } from "../testing.js";

type JazzExtendedOptions = {
  account?: Account | { guest: AnonymousJazzAgent };
  isAuthenticated?: boolean;
};

const customRender = (
  ui: React.ReactNode,
  options: RenderOptions & JazzExtendedOptions = {},
) => {
  const AllTheProviders = ({
    children,
  }: {
    children: React.ReactNode;
  }) => {
    return (
      <JazzTestProvider
        account={options.account}
        isAuthenticated={options.isAuthenticated}
      >
        {children}
      </JazzTestProvider>
    );
  };

  return render(ui, { wrapper: AllTheProviders, ...options });
};

const customRenderHook = <TProps, TResult>(
  callback: (props: TProps) => TResult,
  options: RenderHookOptions<TProps> & JazzExtendedOptions = {},
) => {
  const AllTheProviders = ({
    children,
  }: {
    children: React.ReactNode;
  }) => {
    return (
      <JazzTestProvider
        account={options.account}
        isAuthenticated={options.isAuthenticated}
      >
        {children}
      </JazzTestProvider>
    );
  };

  return renderHook(callback, { wrapper: AllTheProviders, ...options });
};

// re-export everything
export * from "@testing-library/react";

// override render method
export { customRender as render };
export { customRenderHook as renderHook };
