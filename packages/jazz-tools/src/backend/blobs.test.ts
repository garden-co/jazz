import { describe, expect, it, vi } from "vitest";
import { connectedBlobStore, resolveVercelBlob, resolveVercelS3, s3, vercelBlob } from "./blobs.js";
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

  it("is a vercel() core's default once a Blob store is connected", () => {
    const warn = vi.fn();
    expect(resolveCoreBlobs({}, { BLOB_STORE_ID: "abc" }, warn)).toEqual({ storeId: "abc" });
    expect(resolveCoreBlobs({ blobs: "sqlite" }, { BLOB_STORE_ID: "abc" }, warn)).toBeUndefined();
    expect(warn).not.toHaveBeenCalled();
    // Without one, large values stay in SQLite, and the core says how to move them.
    expect(resolveCoreBlobs({}, {}, warn)).toBeUndefined();
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/no Blob store is connected/));
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
