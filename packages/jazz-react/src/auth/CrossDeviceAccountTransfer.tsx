import {
  type UseCrossDeviceAccountTransferAsSourceOptions,
  type UseCrossDeviceAccountTransferAsTargetOptions,
  useAcceptAccountTransferAsSource,
  useAcceptAccountTransferAsTarget,
  useCreateAccountTransferAsSource,
  useCreateAccountTransferAsTarget,
} from "jazz-react-core";
import { useRef } from "react";

export function useCreateAccountTransfer(
  options: {
    as: "source";
  } & Partial<UseCrossDeviceAccountTransferAsSourceOptions>,
): ReturnType<typeof useCreateAccountTransferAsSource>;
export function useCreateAccountTransfer(
  options: {
    as: "target";
  } & Partial<UseCrossDeviceAccountTransferAsTargetOptions>,
): ReturnType<typeof useCreateAccountTransferAsTarget>;
export function useCreateAccountTransfer({
  as: mode,
  ...options
}:
  | ({
      as: "source";
    } & Partial<UseCrossDeviceAccountTransferAsSourceOptions>)
  | ({
      as: "target";
    } & Partial<UseCrossDeviceAccountTransferAsTargetOptions>)) {
  const initialMode = useRef(mode);

  if (initialMode.current !== mode) {
    console.warn(
      "useCreateAccountTransfer mode cannot be changed once mounted.",
    );
  }

  if (initialMode.current === "source") {
    return useCreateAccountTransferAsSource(window.location.origin, options);
  } else {
    return useCreateAccountTransferAsTarget(window.location.origin, options);
  }
}

export function useAcceptAccountTransfer(
  options: {
    as: "source";
  } & Partial<UseCrossDeviceAccountTransferAsSourceOptions>,
): ReturnType<typeof useAcceptAccountTransferAsSource>;
export function useAcceptAccountTransfer(
  options: {
    as: "target";
  } & Partial<UseCrossDeviceAccountTransferAsTargetOptions>,
): ReturnType<typeof useAcceptAccountTransferAsTarget>;
export function useAcceptAccountTransfer({
  as: mode,
  ...options
}:
  | ({
      as: "source";
    } & Partial<UseCrossDeviceAccountTransferAsSourceOptions>)
  | ({
      as: "target";
    } & Partial<UseCrossDeviceAccountTransferAsTargetOptions>)) {
  const initialMode = useRef(mode);

  if (initialMode.current !== mode) {
    console.warn(
      "useAcceptAccountTransfer mode cannot be changed once mounted.",
    );
  }

  if (initialMode.current === "source") {
    return useAcceptAccountTransferAsSource(
      window.location.origin,
      window.location.href,
      options,
    );
  } else {
    return useAcceptAccountTransferAsTarget(
      window.location.origin,
      window.location.href,
      options,
    );
  }
}
