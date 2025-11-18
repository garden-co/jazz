import { co, z } from "jazz-tools";

// #region SchemaUnion
export const ButtonWidget = co.map({
  type: z.literal("button"),
  label: z.string(),
});

export const SliderWidget = co.map({
  type: z.literal("slider"),
  min: z.number(),
  max: z.number(),
});

export const WidgetUnion = co.discriminatedUnion("type", [
  ButtonWidget,
  SliderWidget,
]);
// #endregion SchemaUnion

export const Dashboard = co.map({
  widgets: co.list(WidgetUnion),
});

export const App = co.map({
  name: z.string(),
  dashboards: co.list(Dashboard),
});

// #region NestedUnions
// Define error types
const BadRequestError = co.map({
  status: z.literal("failed"),
  code: z.literal(400),
  message: z.string(),
});

const UnauthorizedError = co.map({
  status: z.literal("failed"),
  code: z.literal(401),
  message: z.string(),
});

const InternalServerError = co.map({
  status: z.literal("failed"),
  code: z.literal(500),
  message: z.string(),
});

// Create a union of error types
const ErrorResponse = co.discriminatedUnion("code", [
  BadRequestError,
  UnauthorizedError,
  InternalServerError,
]);

// Define success type
const SuccessResponse = co.map({
  status: z.literal("success"),
  data: z.string(),
});

// Create a top-level union that includes the error union
const ApiResponse = co.discriminatedUnion("status", [
  SuccessResponse,
  ErrorResponse,
]);

function handleResponse(response: co.loaded<typeof ApiResponse>) {
  if (response.status === "success") {
    console.log("Success:", response.data);
  } else {
    // This is an error - narrow further by error code
    if (response.code === 400) {
      console.log("Bad request:", response.message);
    } else if (response.code === 401) {
      console.log("Unauthorized:", response.message);
    } else if (response.code === 500) {
      console.log("Server error:", response.message);
    }
  }
}
// #endregion
