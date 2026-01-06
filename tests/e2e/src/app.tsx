import React from "react";
import ReactDOM from "react-dom/client";
import { Link, RouterProvider, createBrowserRouter } from "react-router-dom";
import { AuthAndJazz } from "./jazz";
import { ConcurrentChanges } from "./pages/ConcurrentChanges";
import { FileStreamTest } from "./pages/FileStream";
import { InboxPage } from "./pages/Inbox";
import { ResumeSyncState } from "./pages/ResumeSyncState";
import { Sharing } from "./pages/Sharing";
import { TestInput } from "./pages/TestInput";
import { WriteOnlyRole } from "./pages/WriteOnly";
import { SessionLockTest } from "./pages/SessionLock";
import { ConcurrentMigration } from "./pages/ConcurrentMigration";

function Index() {
  return (
    <ul>
      <li>
        <Link to="/test-input">Test Input</Link>
      </li>
      <li>
        <Link to="/resume-sync">Resume Sync</Link>
      </li>
      <li>
        <Link to="/file-stream">File Stream</Link>
      </li>
      <li>
        <Link to="/sharing">Sharing</Link>
      </li>
      <li>
        <Link to="/write-only">Write Only</Link>
      </li>
      <li>
        <Link to="/concurrent-changes">Concurrent Changes</Link>
      </li>
      <li>
        <Link to="/concurrent-migration">Concurrent Migration</Link>
      </li>
      <li>
        <Link to="/inbox">Inbox</Link>
      </li>
      <li>
        <Link to="/session-lock">Session Lock</Link>
      </li>
    </ul>
  );
}

const router = createBrowserRouter(
  [
    {
      path: "/test-input",
      element: <TestInput />,
    },
    {
      path: "/resume-sync",
      element: <ResumeSyncState />,
    },
    {
      path: "/file-stream",
      element: <FileStreamTest />,
    },
    {
      path: "/sharing",
      element: <Sharing />,
    },
    {
      path: "/write-only",
      element: <WriteOnlyRole />,
    },
    {
      path: "/inbox",
      element: <InboxPage />,
    },
    {
      path: "/concurrent-changes",
      element: <ConcurrentChanges />,
    },
    {
      path: "/concurrent-migration",
      element: <ConcurrentMigration />,
    },
    {
      path: "/session-lock",
      element: <SessionLockTest />,
    },
    {
      path: "/",
      element: <Index />,
    },
  ],
  {
    // Enabling these to turn off the console warnings
    future: {
      v7_skipActionErrorRevalidation: true,
      v7_partialHydration: true,
      v7_normalizeFormMethod: true,
      v7_fetcherPersist: true,
      v7_relativeSplatPath: true,
    },
  },
);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AuthAndJazz>
      <RouterProvider
        router={router}
        future={{
          v7_startTransition: true,
        }}
      />
    </AuthAndJazz>
  </React.StrictMode>,
);
