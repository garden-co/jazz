import { describe, expect, it, vi } from "vitest";
import { resolveVercelS3, s3 } from "./blobs.js";

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
