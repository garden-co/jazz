// #region RecordMetrics
import { JazzInspector, recordMetrics } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";

// Initialize metrics collection before rendering
recordMetrics();

function App() {
  return (
    // @ts-expect-error No sync prop
    <JazzReactProvider>
      {/* Your app components */}
      <JazzInspector />
    </JazzReactProvider>
  );
}
// #endregion

export {};
