import { describe, expect, test, beforeEach } from "vitest";
import { createJazzTestAccount } from "jazz-tools/testing";
import { startWebhookService } from "../service.js";
import { JazzWebhook } from "../webhook.js";
import { WebhookServiceResponses } from "../types.js";

describe("WebhookService", () => {
  let service: ReturnType<typeof startWebhookService>;
  let webhook: JazzWebhook;

  beforeEach(async () => {
    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    webhook = new JazzWebhook(JazzWebhook.createRegistry(account));
    service = startWebhookService(webhook);
  });

  test("should have health endpoint", async () => {
    const req = new Request("http://localhost:3000/health");
    const res = await service.fetch(req);
    const data =
      (await res.json()) as WebhookServiceResponses["HealthCheckSuccess"];

    expect(res.status).toBe(200);
    expect(data.success).toBe(true);
    expect(data.message).toBe("Webhook service is running");
    expect(data.data.timestamp).toBeDefined();
    expect(typeof data.data.webhookCount).toBe("number");
  });

  test("should register a webhook", async () => {
    const webhookData = {
      webhookUrl: "https://example.com/webhook",
      coValueId: "co_z1234567890abcdef",
    };

    const req = new Request("http://localhost:3000/webhooks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(webhookData),
    });

    const res = await service.fetch(req);
    const data =
      (await res.json()) as WebhookServiceResponses["RegisterWebhookSuccess"];

    expect(res.status).toBe(201);
    expect(data.success).toBe(true);
    expect(data.data.webhookId).toBeDefined();
    expect(data.data.message).toBe("Webhook registered successfully");
  });

  test("should reject invalid webhook registration", async () => {
    const invalidData = {
      webhookUrl: "not-a-url",
      coValueId: "invalid-id",
    };

    const req = new Request("http://localhost:3000/webhooks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(invalidData),
    });

    const res = await service.fetch(req);
    const data =
      (await res.json()) as WebhookServiceResponses["ValidationError"];

    expect(res.status).toBe(400);
    expect(data.success).toBe(false);
  });

  test("should reject missing required fields", async () => {
    const incompleteData = {
      webhookUrl: "https://example.com/webhook",
      // missing coValueId
    };

    const req = new Request("http://localhost:3000/webhooks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(incompleteData),
    });

    const res = await service.fetch(req);
    const data =
      (await res.json()) as WebhookServiceResponses["ValidationError"];

    expect(res.status).toBe(400);
    expect(data.success).toBe(false);
  });

  test("should get specific webhook by ID", async () => {
    // First register a webhook
    const webhookData = {
      webhookUrl: "https://example.com/webhook",
      coValueId: "co_z1234567890abcdef",
    };

    const registerReq = new Request("http://localhost:3000/webhooks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(webhookData),
    });

    const registerRes = await service.fetch(registerReq);
    const registerData =
      (await registerRes.json()) as WebhookServiceResponses["RegisterWebhookSuccess"];

    expect(registerData.success).toBe(true);
    const webhookId = registerData.data.webhookId;

    // Then get the specific webhook
    const getReq = new Request(`http://localhost:3000/webhooks/${webhookId}`);
    const res = await service.fetch(getReq);
    const data =
      (await res.json()) as WebhookServiceResponses["WebhookInfoSuccess"];

    expect(res.status).toBe(200);
    expect(data.success).toBe(true);
    expect(data.data.id).toBe(webhookId);
    expect(data.data.webhookUrl).toBe(webhookData.webhookUrl);
    expect(data.data.coValueId).toBe(webhookData.coValueId);
  });

  test("should return 404 for non-existent webhook", async () => {
    const req = new Request("http://localhost:3000/webhooks/non-existent-id");
    const res = await service.fetch(req);
    const data =
      (await res.json()) as WebhookServiceResponses["WebhookNotFoundError"];

    expect(res.status).toBe(404);
    expect(data.success).toBe(false);
    expect(data.error).toContain("not found");
  });

  test("should delete a webhook", async () => {
    // First register a webhook
    const webhookData = {
      webhookUrl: "https://example.com/webhook",
      coValueId: "co_z1234567890abcdef",
    };

    const registerReq = new Request("http://localhost:3000/webhooks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(webhookData),
    });

    const registerRes = await service.fetch(registerReq);
    const registerData =
      (await registerRes.json()) as WebhookServiceResponses["RegisterWebhookSuccess"];

    expect(registerData.success).toBe(true);
    const webhookId = registerData.data.webhookId;

    // Then delete the webhook
    const deleteReq = new Request(
      `http://localhost:3000/webhooks/${webhookId}`,
      {
        method: "DELETE",
      },
    );
    const res = await service.fetch(deleteReq);
    const data =
      (await res.json()) as WebhookServiceResponses["DeleteWebhookSuccess"];

    expect(res.status).toBe(200);
    expect(data.success).toBe(true);
    expect(data.message).toContain("unregistered successfully");
  });
});
