import { afterEach, describe, expect, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { usePasskeyBackup } from "../../src/auth-snippets.js";

function PasskeyBackupProbe(props: {
  onRender: (value: ReturnType<typeof usePasskeyBackup>) => void;
}) {
  props.onRender(usePasskeyBackup());
  return null;
}

describe("usePasskeyBackup", () => {
  let root: Root | null = null;
  let container: HTMLDivElement | null = null;

  afterEach(async () => {
    if (root) {
      await act(async () => {
        root!.unmount();
      });
    }

    container?.remove();
    root = null;
    container = null;
  });

  it("always returns a callable backup function", async () => {
    let latest: ReturnType<typeof usePasskeyBackup> | null = null;

    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root!.render(
        <PasskeyBackupProbe
          onRender={(value) => {
            latest = value;
          }}
        />,
      );
    });

    expect(latest).not.toBeNull();
    expect(typeof latest!.backupWithPasskey).toBe("function");
  });
});
