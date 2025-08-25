// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import { Account, FileStream, ImageDefinition } from "../../../";
import { Image } from "../../media/image";
import { createJazzTestAccount } from "../../testing";
import { render, screen, waitFor } from "../testUtils";

describe("Image", async () => {
  const account = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  vi.spyOn(Account, "getMe").mockReturnValue(account);

  describe("initial rendering", () => {
    it("should render nothing if coValue is not found", async () => {
      const { container } = render(
        <Image imageId="co_zMTubMby3QiKDYnW9e2BEXW7Xaq" alt="test" />,
        { account },
      );
      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe(null);
      expect(img!.getAttribute("height")).toBe(null);
      expect(img!.alt).toBe("test");
      expect(img!.src).toBe("");
    });

    it("should render an empty image if the image is not loaded yet", async () => {
      const original = FileStream.create({ owner: account });
      original.start({ mimeType: "image/jpeg" });
      // Don't end original, so it has no chunks

      const im = ImageDefinition.create(
        {
          original,
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(<Image imageId={im.$jazz.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe(null);
      expect(img!.getAttribute("height")).toBe(null);
      expect(img!.alt).toBe("test");
      expect(img!.src).toBe("");
    });

    it("should render the placeholder image if the image is not loaded yet", async () => {
      const placeholderDataUrl =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

      const original = FileStream.create({ owner: account });
      original.start({ mimeType: "image/jpeg" });
      // Don't end original, so it has no chunks

      const im = ImageDefinition.create(
        {
          original,
          originalSize: [100, 100],
          progressive: false,
          placeholderDataURL: placeholderDataUrl,
        },
        {
          owner: account,
        },
      );

      const { container } = render(<Image imageId={im.$jazz.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe(placeholderDataUrl);
    });

    it("should render the original image once loaded", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      render(<Image imageId={im.$jazz.id} alt="test-loading" />, { account });

      await waitFor(() => {
        expect(
          (screen.getByAltText("test-loading") as HTMLImageElement).src,
        ).toBe("blob:test-100");
      });

      expect(createObjectURLSpy).toHaveBeenCalledOnce();
    });
  });

  describe("dimensions", () => {
    it("should render the original image if the width and height are not set", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(<Image imageId={im.$jazz.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe(null);
      expect(img!.getAttribute("height")).toBe(null);
    });

    it("should render the original sizes", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image
          imageId={im.$jazz.id}
          alt="test"
          width="original"
          height="original"
        />,
        {
          account,
        },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe("100");
      expect(img!.getAttribute("height")).toBe("100");
    });

    it("should render the original size keeping the aspect ratio", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image
          imageId={im.$jazz.id}
          alt="test"
          width="original"
          height={300}
        />,
        {
          account,
        },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe("300");
      expect(img!.getAttribute("height")).toBe("300");
    });

    it("should render the width attribute if it is set", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" width={50} />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe("50");
      expect(img!.getAttribute("height")).toBeNull();
    });

    it("should render the height attribute if it is set", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" height={50} />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBeNull();
      expect(img!.getAttribute("height")).toBe("50");
    });

    it("should render the class attribute if it is set", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" className="test-class" />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.classList.contains("test-class")).toBe(true);
    });
  });

  describe("progressive loading", () => {
    it("should render the resized image if progressive loading is enabled", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const original = await createDummyFileStream(500, account);

      const im = ImageDefinition.create(
        {
          original,
          originalSize: [500, 500],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im.$jazz.set("500x500", original);
      im.$jazz.set("256x256", await createDummyFileStream(256, account));

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test-progressive" width={300} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-500",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledOnce();
    });

    it("should show the highest resolution images as they are loaded", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const original = await createDummyFileStream(1920, account);

      const im = ImageDefinition.create(
        {
          original,
          originalSize: [1920, 1080],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im.$jazz.set("1920x1080", original);
      im.$jazz.set("256x256", await createDummyFileStream(256, account));

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test-progressive" width={1024} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-1920",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);

      // Load higher resolution image
      im.$jazz.set("1024x1024", await createDummyFileStream(1024, account));

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-1024",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(2);
    });

    it("should show the best loaded resolution if width is set", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const original = await FileStream.createFromBlob(createDummyBlob(1), {
        owner: account,
      });

      const im = ImageDefinition.create(
        {
          original,
          originalSize: [100, 100],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im.$jazz.set("100x100", original);
      im.$jazz.set("256x256", await createDummyFileStream(256, account));
      im.$jazz.set("1024x1024", await createDummyFileStream(1024, account));

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test-progressive" width={256} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-256",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);
    });

    it("should show the original image if asked resolution matches", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const original = await createDummyFileStream(100, account);
      const im = ImageDefinition.create(
        {
          original,
          originalSize: [100, 100],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im.$jazz.set("100x100", original);
      im.$jazz.set("256x256", await createDummyFileStream(256, account));

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test-progressive" width={100} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-100",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);
    });

    it("should update to a higher resolution image when width/height props are changed at runtime", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const original = await createDummyFileStream(256, account);
      const im = ImageDefinition.create(
        {
          original,
          originalSize: [256, 256],
          progressive: true,
        },
        {
          owner: account,
        },
      );
      im.$jazz.set("256x256", original);
      im.$jazz.set("1024x1024", await createDummyFileStream(1024, account));

      const { container, rerender } = render(
        <Image
          imageId={im.$jazz.id}
          alt="test-dynamic"
          width={256}
          height={256}
        />,
        { account },
      );

      // Initially, should load 256x256
      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-256",
        );
      });
      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);

      rerender(
        <Image
          imageId={im.$jazz.id}
          alt="test-dynamic"
          width={1024}
          height={1024}
        />,
      );

      // After prop change, should load 1024x1024
      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-1024",
        );
      });
      expect(createObjectURLSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe("ref forwarding", () => {
    it("should forward ref to the img element", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const ref = { current: null as HTMLImageElement | null };

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" ref={ref} />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(ref.current).toBe(img);
    });
  });

  describe("lazy loading", () => {
    it("should return an empty png if loading is lazy and placeholder is not set", async () => {
      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" loading="lazy" />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe("blob:test-70");
    });

    it("should load the image when threshold is reached", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
        },
        {
          owner: account,
        },
      );

      const { container } = render(
        <Image imageId={im.$jazz.id} alt="test" loading="lazy" />,
        { account },
      );

      const img = container.querySelector("img");
      // simulate the load event when the browser's viewport reach the image
      img!.dispatchEvent(new Event("load"));

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-100",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(2);
    });
  });
});

function createDummyBlob(size: number): Blob {
  const blob = new Blob([new Uint8Array(size)], { type: "image/png" });
  return blob;
}

function createDummyFileStream(
  size: number,
  account: Awaited<ReturnType<typeof createJazzTestAccount>>,
) {
  return FileStream.createFromBlob(createDummyBlob(size), {
    owner: account,
  });
}
