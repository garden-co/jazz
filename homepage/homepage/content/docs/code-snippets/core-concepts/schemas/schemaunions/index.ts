import { ButtonWidget, SliderWidget, Dashboard, WidgetUnion } from "./schema";

// #region Dashboard
const dashboard = Dashboard.create({
  widgets: [
    ButtonWidget.create({ type: "button", label: "Click me" }),
    SliderWidget.create({ type: "slider", min: 0, max: 100 }),
  ],
});
// #endregion

// #region DashboardFromJSON
const dashboardFromJSON = Dashboard.create({
  widgets: [
    { type: "button", label: "Click me" },
    { type: "slider", min: 0, max: 100 },
  ],
});
// #endregion

// #region NarrowingUnions
dashboard.widgets.forEach((widget) => {
  if (widget.type === "button") {
    console.log(`Button: ${widget.label}`);
  } else if (widget.type === "slider") {
    console.log(`Slider: ${widget.min} to ${widget.max}`);
  }
});
// #endregion

const widgetId = "";
// #region LoadWidget
const widget = await WidgetUnion.load(widgetId);

// Subscribe to updates
const unsubscribe = WidgetUnion.subscribe(widgetId, {}, (widget) => {
  console.log("Widget updated:", widget);
});
// #endregion
