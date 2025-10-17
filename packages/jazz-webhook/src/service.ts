import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import { zValidator } from "@hono/zod-validator";
import { z } from "zod";
import { WebhookRegistry, WebhookRegistration } from "./webhook.js";
import {
  WebhookServiceOptions,
  RegisterWebhookResponse,
  WebhookInfo,
  WebhookServiceResponses,
  WebhookServiceStatusCodes,
} from "./types.js";

// Zod schemas for validation
const registerWebhookSchema = z.object({
  webhookUrl: z.string().url("Invalid webhook URL"),
  coValueId: z.string().min(1, "coValueId is required"),
});

const webhookIdParamSchema = z.object({
  id: z.string().min(1, "Webhook ID is required"),
});

export function startWebhookService(
  webhook: WebhookRegistry,
  options: WebhookServiceOptions = {},
) {
  webhook.start();

  const app = new Hono();

  // Middleware
  if (options.enableCors !== false) {
    app.use("*", cors());
  }

  app.use("*", logger());

  const router = app
    .get(
      "/webhooks/:id",
      zValidator("param", webhookIdParamSchema),
      async (c) => {
        try {
          const { id: webhookId } = c.req.valid("param");
          const webhookRegistration = await WebhookRegistration.load(
            webhookId,
            {
              resolve: {
                successMap: { $each: true },
              },
            },
          );

          if (!webhookRegistration) {
            const errorResponse: WebhookServiceResponses["WebhookNotFoundError"] =
              {
                success: false,
                error: `Webhook with ID ${webhookId} not found`,
              };
            return c.json(errorResponse, WebhookServiceStatusCodes.NOT_FOUND);
          }

          const webhookInfo: WebhookInfo = {
            id: webhookRegistration.$jazz.id,
            webhookUrl: webhookRegistration.webhookUrl,
            coValueId: webhookRegistration.coValueId,
            active: webhookRegistration.active,
            successMap: webhookRegistration.successMap,
          };

          const successResponse: WebhookServiceResponses["WebhookInfoSuccess"] =
            {
              success: true,
              data: webhookInfo,
            };

          return c.json(successResponse);
        } catch (error) {
          const errorResponse: WebhookServiceResponses["InternalServerError"] =
            {
              success: false,
              error: error instanceof Error ? error.message : "Unknown error",
            };
          return c.json(
            errorResponse,
            WebhookServiceStatusCodes.INTERNAL_SERVER_ERROR,
          );
        }
      },
    )
    .post("/webhooks", zValidator("json", registerWebhookSchema), async (c) => {
      try {
        const body = c.req.valid("json");

        const webhookId = await webhook.register(
          body.webhookUrl,
          body.coValueId,
        );

        const response: RegisterWebhookResponse = {
          webhookId,
          message: "Webhook registered successfully",
        };

        const successResponse: WebhookServiceResponses["RegisterWebhookSuccess"] =
          {
            success: true,
            data: response,
          };

        return c.json(successResponse, WebhookServiceStatusCodes.CREATED);
      } catch (error) {
        const errorResponse: WebhookServiceResponses["ValidationError"] = {
          success: false,
          error: error instanceof Error ? error.message : "Unknown error",
        };
        return c.json(errorResponse, WebhookServiceStatusCodes.BAD_REQUEST);
      }
    })
    .delete("/webhooks/:id", zValidator("param", webhookIdParamSchema), (c) => {
      try {
        const { id: webhookId } = c.req.valid("param");
        webhook.unregister(webhookId);

        const successResponse: WebhookServiceResponses["DeleteWebhookSuccess"] =
          {
            success: true,
            message: "Webhook unregistered successfully",
          };
        return c.json(successResponse);
      } catch (error) {
        const errorResponse: WebhookServiceResponses["WebhookNotFoundError"] = {
          success: false,
          error: error instanceof Error ? error.message : "Unknown error",
        };
        return c.json(errorResponse, WebhookServiceStatusCodes.NOT_FOUND);
      }
    })
    .get("/health", (c) => {
      const successResponse: WebhookServiceResponses["HealthCheckSuccess"] = {
        success: true,
        message: "Webhook service is running",
        data: {
          timestamp: new Date().toISOString(),
          webhookCount: Object.keys(webhook.state).length,
        },
      };

      return c.json(successResponse);
    });

  return router;
}

// Export the app type for RPC client usage
export type AppType = ReturnType<typeof startWebhookService>;
