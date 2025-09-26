/**
 * Comprehensive type definitions for Jazz Webhook service
 */

// Request types
export interface WebhookServiceOptions {
  /** Port to run the service on (default: 3000) */
  port?: number;
  /** Host to bind the service to (default: "localhost") */
  host?: string;
  /** Enable CORS for all origins (default: true) */
  enableCors?: boolean;
  /** Maximum number of retry attempts for failed webhooks (default: 5) */
  maxRetries?: number;
}

export interface RegisterWebhookRequest {
  webhookUrl: string;
  coValueId: string;
}

// Data types
export interface WebhookInfo {
  id: string;
  webhookUrl: string;
  coValueId: string;
  active: boolean;
  updates: number;
}

export interface RegisterWebhookResponse {
  webhookId: string;
  message: string;
}

// Test server types
export interface WebhookTestResponse {
  statusCode: number;
  body?: string;
  delay?: number;
}

/**
 * Consolidated interface for all webhook service response types
 */
export interface WebhookServiceResponses {
  // Success responses
  RegisterWebhookSuccess: {
    success: true;
    data: RegisterWebhookResponse;
  };

  WebhookInfoSuccess: {
    success: true;
    data: WebhookInfo;
  };

  HealthCheckSuccess: {
    success: true;
    message: string;
    data: {
      timestamp: string;
      webhookCount: number;
    };
  };

  DeleteWebhookSuccess: {
    success: true;
    message: string;
  };

  // Error responses
  WebhookNotFoundError: {
    success: false;
    error: string;
  };

  ValidationError: {
    success: false;
    error: string;
  };

  InternalServerError: {
    success: false;
    error: string;
  };

  // Generic error response
  GenericError: {
    success: false;
    error: string;
  };
}

/**
 * HTTP status codes used by the webhook service
 */
export enum WebhookServiceStatusCodes {
  OK = 200,
  CREATED = 201,
  BAD_REQUEST = 400,
  NOT_FOUND = 404,
  INTERNAL_SERVER_ERROR = 500,
}
