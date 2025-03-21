import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { describe, expect, test } from "vitest";
import { Account, FileStream, ImageDefinition, co } from "../exports.js";

const Crypto = await WasmCrypto.create();

describe("ImageDefinition", async () => {
  const me = await Account.create({
    creationProps: { name: "Test User" },
    crypto: Crypto,
  });

  test("Construction with basic properties", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
        placeholderDataURL: "data:image/jpeg;base64,...",
      },
      { owner: me },
    );

    expect(imageDef.originalSize).toEqual([1920, 1080]);
    expect(imageDef.placeholderDataURL).toBe("data:image/jpeg;base64,...");
  });

  test("highestResAvailable with no resolutions", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const result = imageDef.highestResAvailable();
    expect(result).toBeUndefined();
  });

  test("highestResAvailable with single resolution", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream = FileStream.create({ owner: me });
    stream.start({ mimeType: "image/jpeg" });
    stream.push(new Uint8Array([1, 2, 3]));
    stream.end();

    imageDef["1920x1080"] = stream;

    const result = imageDef.highestResAvailable();
    expect(result).toBeDefined();
    expect(result?.res).toBe("1920x1080");
    expect(result?.stream).toStrictEqual(stream);
  });

  test("highestResAvailable with multiple resolutions", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream1 = FileStream.create({ owner: me });
    stream1.start({ mimeType: "image/jpeg" });
    stream1.push(new Uint8Array([1, 2, 3]));
    stream1.end();

    const stream2 = FileStream.create({ owner: me });
    stream2.start({ mimeType: "image/jpeg" });
    stream2.push(new Uint8Array([4, 5, 6]));
    stream2.end();

    imageDef["1920x1080"] = stream1;
    imageDef["1280x720"] = stream2;

    const result = imageDef.highestResAvailable();
    expect(result).toBeDefined();
    expect(result?.res).toBe("1920x1080");
    expect(result?.stream).toStrictEqual(stream1);
  });

  test("highestResAvailable with maxWidth option", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream1 = FileStream.create({ owner: me });
    stream1.start({ mimeType: "image/jpeg" });
    stream1.push(new Uint8Array([1, 2, 3]));
    stream1.end();

    const stream2 = FileStream.create({ owner: me });
    stream2.start({ mimeType: "image/jpeg" });
    stream2.push(new Uint8Array([4, 5, 6]));
    stream2.end();

    imageDef["1920x1080"] = stream1;
    imageDef["1280x720"] = stream2;

    const result = imageDef.highestResAvailable({ maxWidth: 1500 });
    expect(result).toBeDefined();
    expect(result?.res).toBe("1280x720");
    expect(result?.stream).toStrictEqual(stream2);
  });

  test("highestResAvailable with missing chunks", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream1 = FileStream.create({ owner: me });
    stream1.start({ mimeType: "image/jpeg" });
    stream1.push(new Uint8Array([1, 2, 3]));
    stream1.end();

    const stream2 = FileStream.create({ owner: me });
    stream2.start({ mimeType: "image/jpeg" });
    // Don't end stream2, so it has no chunks

    imageDef["1920x1080"] = stream1;
    imageDef["1280x720"] = stream2;

    const result = imageDef.highestResAvailable();
    expect(result).toBeDefined();
    expect(result?.res).toBe("1920x1080");
    expect(result?.stream).toStrictEqual(stream1);
  });

  test("highestResAvailable with missing chunks in middle stream", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream1 = FileStream.create({ owner: me });
    stream1.start({ mimeType: "image/jpeg" });
    stream1.push(new Uint8Array([1, 2, 3]));
    stream1.end();

    const stream2 = FileStream.create({ owner: me });
    stream2.start({ mimeType: "image/jpeg" });
    // Don't end stream2, so it has no chunks

    const stream3 = FileStream.create({ owner: me });
    stream3.start({ mimeType: "image/jpeg" });
    stream3.push(new Uint8Array([7, 8, 9]));
    stream3.end();

    imageDef["1920x1080"] = stream1;
    imageDef["1280x720"] = stream2;
    imageDef["1024x576"] = stream3;

    const result = imageDef.highestResAvailable();
    expect(result).toBeDefined();
    expect(result?.res).toBe("1920x1080");
    expect(result?.stream).toStrictEqual(stream1);
  });

  test("highestResAvailable with non-resolution keys", () => {
    const imageDef = ImageDefinition.create(
      {
        originalSize: [1920, 1080],
      },
      { owner: me },
    );

    const stream = FileStream.create({ owner: me });
    stream.start({ mimeType: "image/jpeg" });
    stream.push(new Uint8Array([1, 2, 3]));
    stream.end();

    // @ts-expect-error - Testing invalid key
    imageDef["invalid-key"] = stream;

    const result = imageDef.highestResAvailable();
    expect(result).toBeUndefined();
  });
});
