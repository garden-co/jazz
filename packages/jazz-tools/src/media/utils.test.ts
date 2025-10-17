import { Account, FileStream, Group, ImageDefinition } from "jazz-tools";
import {
  createJazzTestAccount,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { highestResAvailable, loadImageBySize } from "./utils.js";

const createFileStream = (account: any, blobSize?: number) => {
  return FileStream.createFromBlob(
    new Blob([new Uint8Array(blobSize || 1)], { type: "image/png" }),
    {
      owner: account,
    },
  );
};

describe("highestResAvailable", async () => {
  let account: Account;

  beforeEach(async () => {
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    vi.spyOn(Account, "getMe").mockReturnValue(account);
    await setupJazzTestSync();
  });

  it("returns original if progressive is false", async () => {
    const original = await createFileStream(account.$jazz.owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: false,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("1920x1080", original);

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.image.$jazz.id).toBe(original.$jazz.id);
  });

  it("returns original if progressive is true but no resizes present", async () => {
    const original = await createFileStream(account.$jazz.owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("1920x1080", original);

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.image.$jazz.id).toBe(original.$jazz.id);
  });

  it("returns closest available resize if progressive is true", async () => {
    const original = await createFileStream(account.$jazz.owner);
    const resize256 = await createFileStream(account.$jazz.owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("1920x1080", original);
    imageDef.$jazz.set("256x256", resize256);

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.image.$jazz.id).toBe(resize256.$jazz.id);
  });

  it("returns original if wanted size matches original size", async () => {
    const original = await createFileStream(account.$jazz.owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1024, 1024],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("1024x1024", original);

    const result = highestResAvailable(imageDef, 1024, 1024);
    expect(result?.image.$jazz.id).toBe(original.$jazz.id);
  });

  it("returns best fit among multiple resizes", async () => {
    const original = await createFileStream(account.$jazz.owner);
    const resize256 = await createFileStream(account.$jazz.owner, 1);
    const resize1024 = await createFileStream(account.$jazz.owner, 1);
    const resize2048 = await createFileStream(account.$jazz.owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [2048, 2048],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("256x256", resize256);
    imageDef.$jazz.set("1024x1024", resize1024);
    imageDef.$jazz.set("2048x2048", resize2048);

    // Closest to 900x900 is 1024
    const result = highestResAvailable(imageDef, 900, 900);
    expect(result?.image.$jazz.id).toBe(resize1024.$jazz.id);
  });

  it("returns the best fit resolution", async () => {
    const original = await createFileStream(account.$jazz.owner, 1);
    const resize256 = await createFileStream(account.$jazz.owner, 1);
    const resize2048 = await createFileStream(account.$jazz.owner, 1);
    // 1024 is not loaded yet
    const resize1024 = FileStream.create({ owner: account.$jazz.owner });
    resize1024.start({ mimeType: "image/jpeg" });
    // Don't end resize1024, so it has no chunks

    const imageDef = ImageDefinition.create(
      {
        originalSize: [2048, 2048],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );
    imageDef.$jazz.set("256x256", resize256);
    imageDef.$jazz.set("1024x1024", resize1024);
    imageDef.$jazz.set("2048x2048", resize2048);

    // Closest to 900x900 is 1024
    const result = highestResAvailable(imageDef, 900, 900);
    expect(result?.image.$jazz.id).toBe(resize2048.$jazz.id);
  });

  it("returns original if no resizes are loaded (missing chunks)", async () => {
    const original = await createFileStream(account.$jazz.owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [256, 256],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set("256x256", original);
    // 1024 is not loaded yet
    const resize1024 = FileStream.create({ owner: account.$jazz.owner });
    resize1024.start({ mimeType: "image/jpeg" });
    // Don't end resize1024, so it has no chunks
    imageDef.$jazz.set("1024x1024", resize1024);

    const result = highestResAvailable(imageDef, 1024, 1024);
    // Only original is valid
    expect(result?.image.$jazz.id).toBe(original.$jazz.id);
  });

  it("returns the first loaded resize if original is not loaded yet(missing chunks)", async () => {
    const original = FileStream.create({ owner: account.$jazz.owner });
    original.start({ mimeType: "image/jpeg" });
    // Don't call .end(), so it has no chunks

    const imageDef = ImageDefinition.create(
      {
        originalSize: [300, 300],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set(
      "256x256",
      await createFileStream(account.$jazz.owner, 1),
    );

    const result = highestResAvailable(imageDef, 1024, 1024);
    // Only original is valid
    expect(result?.image.$jazz.id).toBe(imageDef["256x256"]!.$jazz.id);
  });

  it("returns the highest resolution if no good match is found", async () => {
    const original = await createFileStream(account.$jazz.owner, 1);

    const imageDef = ImageDefinition.create(
      {
        originalSize: [300, 300],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );

    imageDef.$jazz.set(
      "256x256",
      await createFileStream(account.$jazz.owner, 1),
    );
    imageDef.$jazz.set("300x300", original);

    const result = highestResAvailable(imageDef, 1024, 1024);
    expect(result?.image.$jazz.id).toBe(original.$jazz.id);
  });
});

describe("loadImageBySize", async () => {
  let account: Account;
  beforeEach(async () => {
    account = await setupJazzTestSync();
    setActiveAccount(account);
  });

  const createImageDef = async (
    sizes: Array<[number, number]>,
    progressive = true,
    owner: Account | Group = account,
  ) => {
    if (sizes.length === 0) throw new Error("sizes array must not be empty");

    const originalSize = sizes[sizes.length - 1]!;
    sizes = sizes.slice(0, -1);

    const original = await createFileStream(owner, 1);
    // Ensure sizes array is not empty
    const imageDef = ImageDefinition.create(
      {
        originalSize,
        progressive,
        original,
      },
      { owner },
    );
    imageDef.$jazz.set(`${originalSize[0]}x${originalSize[1]}`, original);

    for (const size of sizes) {
      if (!size) continue;
      const [w, h] = size;
      imageDef.$jazz.set(`${w}x${h}`, await createFileStream(owner, 1));
    }
    return imageDef;
  };

  it("return null if the image cannot be loaded", async () => {
    const result = await loadImageBySize("invalid-id", 256, 256);
    expect(result).toBeNull();
  });

  it("returns original if progressive is false", async () => {
    const imageDef = await createImageDef([[1920, 1080]], false);
    const result = await loadImageBySize(imageDef, 256, 256);
    expect(result?.image.$jazz.id).toBe(imageDef["1920x1080"]!.$jazz.id);
  });

  it("returns the original image already loaded", async () => {
    const account = await setupJazzTestSync({ asyncPeers: true });
    const account2 = await createJazzTestAccount();

    setActiveAccount(account);

    const group = Group.create();
    group.addMember("everyone", "reader");

    const imageDef = await createImageDef([[1920, 1080]], false, group);
    setActiveAccount(account2);

    const result = await loadImageBySize(imageDef, 256, 256);
    expect(result?.image.$jazz.id).toBe(imageDef["1920x1080"]!.$jazz.id);
    expect(result?.image.isBinaryStreamEnded()).toBe(true);
    expect(result?.image.asBase64()).toStrictEqual(expect.any(String));
  });

  it("returns null if no sizes are available", async () => {
    const original = await createFileStream(account.$jazz.owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );
    const result = await loadImageBySize(imageDef, 256, 256);
    expect(result).toBeNull();
  });

  it("returns the closest available resize if progressive is true", async () => {
    const imageDef = await createImageDef([
      [256, 256],
      [1920, 1080],
    ]);
    const result = await loadImageBySize(imageDef.$jazz.id, 256, 256);
    expect(result?.image.$jazz.id).toBe(imageDef["256x256"]!.$jazz.id);
    expect(result?.width).toBe(256);
    expect(result?.height).toBe(256);
  });

  it("returns the best fit among multiple resizes", async () => {
    const imageDef = await createImageDef([
      [256, 256],
      [1024, 1024],
      [2048, 2048],
    ]);
    const result = await loadImageBySize(imageDef, 900, 900);
    expect(result?.image.$jazz.id).toBe(imageDef["1024x1024"]!.$jazz.id);
    expect(result?.width).toBe(1024);
    expect(result?.height).toBe(1024);
  });

  it("returns the highest resolution if no good match is found", async () => {
    const imageDef = await createImageDef([
      [256, 256],
      [300, 300],
    ]);
    const result = await loadImageBySize(imageDef, 1024, 1024);
    expect(result?.image.$jazz.id).toBe(imageDef["300x300"]!.$jazz.id);
    expect(result?.width).toBe(300);
    expect(result?.height).toBe(300);
  });

  it("returns null if the best target is not loaded", async () => {
    const original = await createFileStream(account.$jazz.owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [256, 256],
        progressive: true,
        original,
      },
      { owner: account.$jazz.owner },
    );
    // No resizes added
    const result = await loadImageBySize(imageDef, 1024, 1024);
    expect(result).toBeNull();
  });

  it("returns the correct size when wanted size matches available size exactly", async () => {
    const imageDef = await createImageDef([
      [512, 512],
      [1024, 1024],
    ]);
    const result = await loadImageBySize(imageDef, 1024, 1024);
    expect(result?.image.$jazz.id).toBe(imageDef["1024x1024"]!.$jazz.id);
    expect(result?.width).toBe(1024);
    expect(result?.height).toBe(1024);
  });

  it("returns the image already loaded", async () => {
    const account = await setupJazzTestSync({ asyncPeers: true });

    setActiveAccount(account);

    const group = Group.create();
    group.addMember("everyone", "reader");

    const imageDef = await createImageDef(
      [
        [512, 512],
        [1024, 1024],
      ],
      undefined,
      group,
    );

    const account2 = await createJazzTestAccount();
    setActiveAccount(account2);

    const result = await loadImageBySize(imageDef.$jazz.id, 1024, 1024);
    expect(result).not.toBeNull();
    expect(result?.image.$jazz.id).toBe(imageDef["1024x1024"]!.$jazz.id);
    expect(result?.image.isBinaryStreamEnded()).toBe(true);
    expect(result?.image.asBase64()).toStrictEqual(expect.any(String));
  });
});
