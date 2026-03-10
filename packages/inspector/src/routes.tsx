import { Navigate, Route, Routes } from "react-router";
import { DataExplorer } from "./pages/data-explorer";
import { TableDataGrid } from "./components/data-explorer/TableDataGrid";
import { TableSchemaSql } from "./components/data-explorer/TableSchemaSql";
import { InspectorLayout } from "./components/inspector-layout";
import { LiveQuery } from "./pages/live-query";
import { useDevtoolsContext } from "./contexts/devtools-context";

export function InspectorRoutes() {
  const { runtime } = useDevtoolsContext();

  return (
    <Routes>
      <Route path="/" element={<InspectorLayout />}>
        <Route index element={<Navigate to="/data-explorer" replace />} />
        <Route path="data-explorer" element={<DataExplorer />}>
          <Route path=":table/data" element={<TableDataGrid />} />
          <Route path=":table/schema" element={<TableSchemaSql />} />
        </Route>
        {runtime === "extension" ? <Route path="live-query" element={<LiveQuery />} /> : null}
      </Route>
    </Routes>
  );
}
