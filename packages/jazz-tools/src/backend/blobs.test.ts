import { describe, expect, it, vi } from "vitest";
import {
  files,
  connectedBlobStore,
  resolveVercelBlob,
  resolveVercelS3,
  s3,
  vercelBlob,
} from "./blobs.js";
import { environmentOf, resolveCoreBlobs } from "./vercel-store.js";

describe("s3()", () => {
  it("keeps only the options given", () => {
    expect(s3()).toEqual({ kind: "s3" });
    expect(s3({ bucket: "b", prefix: undefined })).toEqual({ kind: "s3", bucket: "b" });
  });

  it("takes a vercel() core's bucket from the Marketplace environment", () => {
    const warn = vi.fn();
    const env = {
      S3_BUCKET_NAME: "app-blobs",
      AWS_REGION: "us-east-1",
      AWS_ROLE_ARN: "arn:aws:iam::1:role/vercel",
    };
    expect(resolveVercelS3(s3(), "iad1", env, warn)).toEqual({
      bucket: "app-blobs",
      region: "us-east-1",
      roleArn: "arn:aws:iam::1:role/vercel",
    });
    expect(warn).not.toHaveBeenCalled();
    expect(resolveVercelS3(s3({ prefix: "p/" }), "fra1", env, warn).prefix).toBe("p/");
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/us-east-1.*fra1 \(eu-central-1\)/));
  });

  it("names what a vercel() core is missing", () => {
    expect(() => resolveVercelS3(s3(), "iad1", {})).toThrow(
      /S3_BUCKET_NAME .*AWS_REGION .*AWS_ROLE_ARN not set.*Vercel Marketplace/,
    );
  });
});

describe("vercelBlob()", () => {
  it("finds the project's Blob store by BLOB_STORE_ID only", () => {
    expect(connectedBlobStore({ BLOB_STORE_ID: "store_abc" })).toBe("abc");
    // A read-write token alone is not enough: the core authenticates with OIDC.
    expect(
      connectedBlobStore({ BLOB_READ_WRITE_TOKEN: "vercel_blob_rw_xyz_secret" }),
    ).toBeUndefined();
    expect(connectedBlobStore({})).toBeUndefined();
    expect(resolveVercelBlob(vercelBlob(), { BLOB_STORE_ID: "abc" })).toEqual({ storeId: "abc" });
    expect(
      resolveVercelBlob(vercelBlob({ storeId: "store_own", prefix: "p/" }), {
        BLOB_STORE_ID: "abc",
      }),
    ).toEqual({ storeId: "own", prefix: "p/" });
    expect(() => resolveVercelBlob(vercelBlob(), {})).toThrow(/BLOB_STORE_ID is not set/);
  });

  it("requires an explicit destination even with a connected Blob store", () => {
    const env = { BLOB_STORE_ID: "abc" };
    // @ts-expect-error JavaScript callers can omit blobs.
    expect(() => resolveCoreBlobs({ region: "iad1" }, env)).toThrow(/blobs is required/);
    expect(resolveCoreBlobs({ blobs: "sqlite", region: "iad1" }, env)).toBeUndefined();
    expect(resolveCoreBlobs({ blobs: vercelBlob(), region: "iad1" }, env)).toEqual({
      storeId: "abc",
    });
  });

  it("keeps Sandbox blob directories on its persistent Drive", () => {
    const destination = (dir: string) =>
      resolveCoreBlobs({ region: "iad1", blobs: files({ dir }) }, {});
    expect(destination("chunks")).toEqual(files({ dir: "/data/store/chunks" }));
    expect(destination("/data/shared/chunks")).toEqual(files({ dir: "/data/shared/chunks" }));
    expect(() => destination("../../tmp")).toThrow(/persistent|Drive/);
  });

  it("checks an s3() bucket against the core's configured region", () => {
    const warn = vi.fn();
    const env = {
      S3_BUCKET_NAME: "app-blobs",
      AWS_REGION: "eu-central-1",
      AWS_ROLE_ARN: "arn:aws:iam::1:role/vercel",
    };
    resolveCoreBlobs({ blobs: s3(), region: "fra1" }, env, warn);
    expect(warn).not.toHaveBeenCalled();
    resolveCoreBlobs({ blobs: s3(), region: "iad1" }, env, warn);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/eu-central-1.*iad1 \(us-east-1\)/));
  });
});

describe("environmentOf", () => {
  const trust = (subject: string) => ({ issuer: "i", audience: "a", subject });

  it("names the environment a core's blob store is claimed for", () => {
    expect(environmentOf(trust("owner:t:project:p:environment:production"))).toBe("production");
    expect(environmentOf(trust("owner:t:project:p:environment:preview"))).toBe("preview");
    expect(environmentOf(trust("owner:t:project:p:environment:staging"))).toBe("staging");
    expect(environmentOf(trust("some-other-workload"))).toBeUndefined();
  });
});
