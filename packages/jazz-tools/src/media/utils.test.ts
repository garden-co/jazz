import { FileStream, ImageDefinition } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { describe, expect, it } from "vitest";
import { highestResAvailable } from "./utils";

const createFileStream = (account: any, blobSize?: number) => {
  return FileStream.createFromBlob(
    new Blob([new Uint8Array(blobSize || 1)], { type: "image/png" }),
    {
      owner: account,
    },
  );
};

describe("highestResAvailable", async () => {
  const account = await createJazzTestAccount();

  it("returns original if progressive is false", async () => {
    const original = await createFileStream(account._owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: false,
        original,
      },
      { owner: account._owner },
    );

    imageDef["1920x1080"] = original;

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.id).toBe(original.id);
  });

  it("returns original if progressive is true but no resizes present", async () => {
    const original = await createFileStream(account._owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["1920x1080"] = original;

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.id).toBe(original.id);
  });

  it("returns closest available resize if progressive is true", async () => {
    const original = await createFileStream(account._owner);
    const resize256 = await createFileStream(account._owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["1920x1080"] = original;
    imageDef["256x256"] = resize256;

    const result = highestResAvailable(imageDef, 256, 256);
    expect(result?.id).toBe(resize256.id);
  });

  it("returns original if wanted size matches original size", async () => {
    const original = await createFileStream(account._owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1024, 1024],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["1024x1024"] = original;

    const result = highestResAvailable(imageDef, 1024, 1024);
    expect(result?.id).toBe(original.id);
  });

  it("returns best fit among multiple resizes", async () => {
    const original = await createFileStream(account._owner);
    const resize256 = await createFileStream(account._owner, 1);
    const resize1024 = await createFileStream(account._owner, 1);
    const resize2048 = await createFileStream(account._owner, 1);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [2048, 2048],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["256x256"] = resize256;
    imageDef["1024x1024"] = resize1024;
    imageDef["2048x2048"] = resize2048;

    // Closest to 900x900 is 1024
    const result = highestResAvailable(imageDef, 900, 900);
    expect(result?.id).toBe(resize1024.id);
  });

  it("returns the higher available resolution", async () => {
    const original = await createFileStream(account._owner, 1);
    const resize256 = await createFileStream(account._owner, 1);
    const resize2048 = await createFileStream(account._owner, 1);
    // 1024 is not loaded yet
    const resize1024 = FileStream.create({ owner: account._owner });
    resize1024.start({ mimeType: "image/jpeg" });
    // Don't end resize1024, so it has no chunks

    const imageDef = ImageDefinition.create(
      {
        originalSize: [2048, 2048],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );
    imageDef["256x256"] = resize256;
    imageDef["1024x1024"] = resize1024;
    imageDef["2048x2048"] = resize2048;

    // Closest to 900x900 is 1024
    const result = highestResAvailable(imageDef, 900, 900);
    expect(result?.id).toBe(resize2048.id);
  });

  it("returns original if no resizes are loaded (missing chunks)", async () => {
    const original = await createFileStream(account._owner);
    const imageDef = ImageDefinition.create(
      {
        originalSize: [256, 256],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["256x256"] = original;
    // 1024 is not loaded yet
    const resize1024 = FileStream.create({ owner: account._owner });
    resize1024.start({ mimeType: "image/jpeg" });
    // Don't end resize1024, so it has no chunks
    imageDef["1024x1024"] = resize1024;

    const result = highestResAvailable(imageDef, 1024, 1024);
    // Only original is valid
    expect(result?.id).toBe(original.id);
  });

  it("returns the first loaded resize if original is not loaded yet(missing chunks)", async () => {
    const original = FileStream.create({ owner: account._owner });
    original.start({ mimeType: "image/jpeg" });
    // Don't call .end(), so it has no chunks

    const imageDef = ImageDefinition.create(
      {
        originalSize: [300, 300],
        progressive: true,
        original,
      },
      { owner: account._owner },
    );

    imageDef["256x256"] = await createFileStream(account._owner, 1);

    const result = highestResAvailable(imageDef, 1024, 1024);
    // Only original is valid
    expect(result?.id).toBe(imageDef["256x256"].id);
  });

  it.todo("returns the correct size based on aspect ratio and maxWidth");
});
