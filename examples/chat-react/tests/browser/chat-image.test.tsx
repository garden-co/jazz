import { describe, it, expect, afterEach, vi } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { IncompleteFileDataError } from "jazz-tools";

const loadFileAsBlob = vi.fn();

vi.mock("jazz-tools/react", async () => {
  const actual = await vi.importActual<typeof import("jazz-tools/react")>("jazz-tools/react");

  return {
    ...actual,
    useDb: () => ({ loadFileAsBlob }),
  };
});

import { ChatImage } from "../../src/components/chat/ChatImage.js";

function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  return new Promise((resolve, reject) => {
    const poll = () => {
      if (check()) {
        resolve();
        return;
      }

      if (Date.now() >= deadline) {
        reject(new Error(`Timeout: ${message}`));
        return;
      }

      window.setTimeout(poll, 25);
    };

    poll();
  });
}

describe("ChatImage", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  afterEach(async () => {
    loadFileAsBlob.mockReset();
    vi.restoreAllMocks();

    for (const { root, container } of mounts) {
      try {
        await act(async () => root.unmount());
      } catch {
        /* best effort */
      }

      container.remove();
    }

    mounts.length = 0;
  });

  it("retries transient file-read misses until the image becomes available", async () => {
    loadFileAsBlob
      .mockRejectedValueOnce(
        new IncompleteFileDataError(
          "file-1",
          "missing-part",
          'File "file-1" is incomplete: missing part 0 (part-1) at the requested query tier.',
          { partId: "part-1", partIndex: 0 },
        ),
      )
      .mockResolvedValueOnce(new Blob([Uint8Array.from([1, 2, 3])], { type: "image/png" }));

    const objectUrl = "blob:test-image";
    vi.spyOn(URL, "createObjectURL").mockReturnValue(objectUrl);
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    mounts.push({ root, container });

    await act(async () => {
      root.render(
        <ChatImage
          attachment={
            {
              id: "att-1",
              fileId: "file-1",
              name: "test-image.png",
              type: "image",
              size: 3,
            } as any
          }
        />,
      );
    });

    await waitFor(
      () => container.querySelector("img")?.getAttribute("src") === objectUrl,
      3000,
      "image should render after a transient file-read miss",
    );

    expect(loadFileAsBlob.mock.calls.length).toBeGreaterThanOrEqual(2);
  });
});
