// @vitest-environment happy-dom
import { FileStream, ImageDefinition } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { describe, expect, it, vi } from "vitest";
import Image from "../../media/image.svelte";
import type { ImageProps } from "../../media/image.types.js";
import { render, screen, waitFor } from "../testUtils";

describe("Image", async () => {
  const account = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const renderWithAccount = (props: ImageProps) =>
    render(Image, props, { account });

  describe("initial rendering", () => {
    it("should render a blank placeholder while waiting for the coValue to load", async () => {
      const { container } = renderWithAccount({
        imageId: "co_zMTubMby3QiKDYnW9e2BEXW7Xaq",
        alt: "test",
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe(null);
      expect(img!.getAttribute("height")).toBe(null);
      expect(img!.alt).toBe("test");
      expect(img!.src).toBe("data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==");
    });

    it("should render nothing if coValue is not found", async () => {
      const { container } = renderWithAccount({
        imageId: "co_zMTubMby3QiKDYnW9e2BEXW7Xaq",
        alt: "test",
      });

      await waitFor(() => {
        const img = container.querySelector("img");
        expect(img).toBeDefined();
        expect(img!.getAttribute("width")).toBe(null);
        expect(img!.getAttribute("height")).toBe(null);
        expect(img!.alt).toBe("test");
        expect(img!.src).toBe("");
      });
    });

    it("should render an empty image if the image is not loaded yet", async () => {
      const original = FileStream.create({ owner: account.$jazz.owner });
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
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

      const original = FileStream.create({ owner: account.$jazz.owner });
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe(placeholderDataUrl);
    });

    it("should not override actual placeholders", async () => {
      const placeholderDataUrl =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
      const customPlaceholder =
        "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIGNsYXNzPSJsdWNpZGUgbHVjaWRlLXVzZXItaWNvbiBsdWNpZGUtdXNlciI+PHBhdGggZD0iTTE5IDIxdi0yYTQgNCAwIDAgMC00LTRIOWE0IDQgMCAwIDAtNCA0djIiLz48Y2lyY2xlIGN4PSIxMiIgY3k9IjciIHI9IjQiLz48L3N2Zz4=";

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        placeholder: customPlaceholder
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe(placeholderDataUrl);
    });

    it("should show custom placeholder while loading and replace with loaded image", async () => {
      const createObjectURLSpy = vi
        .spyOn(URL, "createObjectURL")
        .mockImplementation((blob) => {
          if (!(blob instanceof Blob)) {
            throw new Error("Blob expected");
          }
          return `blob:test-${blob.size}`;
        });

      const customPlaceholder =
        "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIGNsYXNzPSJsdWNpZGUgbHVjaWRlLXVzZXItaWNvbiBsdWNpZGUtdXNlciI+PHBhdGggZD0iTTE5IDIxdi0yYTQgNCAwIDAgMC00LTRIOWE0IDQgMCAwIDAtNCA0djIiLz48Y2lyY2xlIGN4PSIxMiIgY3k9IjciIHI9IjQiLz48L3N2Zz4=";

      // Create an image with no chunks initially (loading state)
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-loading-custom-placeholder",
        placeholder: customPlaceholder
      });
      // Initially should show custom placeholder
      let img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe(customPlaceholder);

      // Now add the actual image data
      const imageData = await createDummyFileStream(100, account);
      im.$jazz.set("100x100", imageData);

      // Wait for the image to load and replace the placeholder
      await waitFor(() => {
        img = container.querySelector("img");
        expect(img!.src).toBe("blob:test-100");
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);
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

      renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-loading",
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        width: "original",
        height: "original",
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        width: "original",
        height: 300,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        width: 50,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        height: 50,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        class: "test-class",
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-progressive",
        width: 300,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-progressive",
        width: 1024,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-progressive",
        width: 256,
      });

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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-progressive",
        width: 100,
      });

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

      const { container, rerender } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test-dynamic",
        width: 256,
        height: 256,
      });

      // Initially, should load 256x256
      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-256",
        );
      });
      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);

      rerender({
        imageId: im.$jazz.id,
        alt: "test-dynamic",
        width: 1024,
        height: 1024,
      });

      // After prop change, should load 1024x1024
      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-1024",
        );
      });
      expect(createObjectURLSpy).toHaveBeenCalledTimes(2);
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        loading: "lazy",
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.src).toBe("blob:test-68");
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

      const { container } = renderWithAccount({
        imageId: im.$jazz.id,
        alt: "test",
        loading: "lazy",
      });

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
