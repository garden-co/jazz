// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import { FileStream, ImageDefinition } from "../../../tools";
import Image from "../../media/image";
import { createJazzTestAccount } from "../../testing";
import { render, screen, waitFor } from "../testUtils";

describe("Image", () => {
  describe("initial rendering", () => {
    it("should render nothing if coValue is not found", async () => {
      const account = await createJazzTestAccount();

      const { container } = render(
        <Image imageId="co_zMTubMby3QiKDYnW9e2BEXW7Xaq" alt="test" />,
        { account },
      );
      const img = container.querySelector("img");
      expect(img).toBeNull();
    });

    it("should render an empty image with original sizes if the image is not loaded yet", async () => {
      const account = await createJazzTestAccount();

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

      const { container } = render(<Image imageId={im.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.width).toBe(100);
      expect(img!.height).toBe(100);
      expect(img!.alt).toBe("test");
      expect(img!.src).toBe("");
    });

    it("should render the placeholder image if the image is not loaded yet", async () => {
      const account = await createJazzTestAccount();

      const placeholderDataUrl =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

      const im = ImageDefinition.create(
        {
          original: await createDummyFileStream(100, account),
          originalSize: [100, 100],
          progressive: false,
          placeholderDataURL: placeholderDataUrl,
        },
        {
          owner: account,
        },
      );

      const { container } = render(<Image imageId={im.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.width).toBe(100);
      expect(img!.height).toBe(100);
      expect(img!.src).toBe(placeholderDataUrl);
    });

    it("should render the original image once loaded", async () => {
      const account = await createJazzTestAccount();

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

      render(<Image imageId={im.id} alt="test-loading" />, { account });

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
      const account = await createJazzTestAccount();

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

      const { container } = render(<Image imageId={im.id} alt="test" />, {
        account,
      });

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.width).toBe(100);
      expect(img!.height).toBe(100);
    });

    it("should render the width attribute if it is set", async () => {
      const account = await createJazzTestAccount();

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
        <Image imageId={im.id} alt="test" width={50} />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBe("50");
      expect(img!.getAttribute("height")).toBeNull();
    });

    it("should render the height attribute if it is set", async () => {
      const account = await createJazzTestAccount();

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
        <Image imageId={im.id} alt="test" height={50} />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.getAttribute("width")).toBeNull();
      expect(img!.getAttribute("height")).toBe("50");
    });

    it("should render the class attribute if it is set", async () => {
      const account = await createJazzTestAccount();

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
        <Image imageId={im.id} alt="test" className="test-class" />,
        { account },
      );

      const img = container.querySelector("img");
      expect(img).toBeDefined();
      expect(img!.classList.contains("test-class")).toBe(true);
    });
  });

  describe("progressive loading", () => {
    it("should render the resized image if progressive loading is enabled", async () => {
      const account = await createJazzTestAccount();

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
          original: await createDummyFileStream(1, account),
          originalSize: [100, 100],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im["256"] = await createDummyFileStream(256, account);

      const { container } = render(
        <Image imageId={im.id} alt="test-progressive" />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-256",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledOnce();
    });

    it.only("should show the highest resolution images as they are loaded", async () => {
      const account = await createJazzTestAccount();

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
          original: await createDummyFileStream(1, account),
          originalSize: [1920, 1080],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im["256"] = await createDummyFileStream(256, account);

      const { container } = render(
        <Image imageId={im.id} alt="test-progressive" width={1024} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-256",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);

      // Load higher resolution image
      im["1024"] = await createDummyFileStream(1024, account);

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-1024",
        );
      });

      // Setting the property 1024 from scratch changes the `image` identity in useCoState
      // TODO: find a way to mock getChunks
      expect(createObjectURLSpy).toHaveBeenCalledTimes(3);
    });

    it("should show the best loaded resolution if width is set", async () => {
      const account = await createJazzTestAccount();

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
          original: await FileStream.createFromBlob(createDummyBlob(1), {
            owner: account,
          }),
          originalSize: [100, 100],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im["256"] = await createDummyFileStream(256, account);
      im["1024"] = await createDummyFileStream(1024, account);

      const { container } = render(
        <Image imageId={im.id} alt="test-progressive" width={256} />,
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
      const account = await createJazzTestAccount();

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
          original: await FileStream.createFromBlob(createDummyBlob(100), {
            owner: account,
          }),
          originalSize: [100, 100],
          progressive: true,
        },
        {
          owner: account,
        },
      );

      im["256"] = await createDummyFileStream(256, account);

      const { container } = render(
        <Image imageId={im.id} alt="test-progressive" width={100} />,
        { account },
      );

      await waitFor(() => {
        expect((container.querySelector("img") as HTMLImageElement).src).toBe(
          "blob:test-100",
        );
      });

      expect(createObjectURLSpy).toHaveBeenCalledTimes(1);
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
