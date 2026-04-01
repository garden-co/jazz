import { Navigate, Route, Routes } from "react-router";
import { DataExplorer } from "./pages/data-explorer";
import { TableDataGrid } from "./components/data-explorer/TableDataGrid";
import { TableSchemaDefinition } from "./components/data-explorer/TableSchemaDefinition";
import { InspectorLayout } from "./components/inspector-layout";
import { LiveQuery } from "./pages/live-query";

export function InspectorRoutes() {
  return (
    <Routes>
      <Route path="/" element={<InspectorLayout />}>
        <Route index element={<Navigate to="/data-explorer" replace />} />
        <Route path="data-explorer" element={<DataExplorer />}>
          <Route path=":table/data" element={<TableDataGrid />} />
          <Route path=":table/schema" element={<TableSchemaDefinition />} />
        </Route>
        <Route path="live-query" element={<LiveQuery />} />
      </Route>
    </Routes>
  );
}
