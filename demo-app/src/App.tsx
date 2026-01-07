import { useState, useEffect, useRef } from "react";
import { JazzProvider, useJazz, useAll, type WasmDatabaseLike } from "@jazz/react";
import { app } from "./generated/client";
import type { IssueWith } from "./generated/types";

// Type for an issue with all includes loaded
export type LoadedIssue = IssueWith<{
  project: true;
  IssueLabels: { label: true };
  IssueAssignees: { user: true };
}>;

// @ts-ignore - vite handles ?raw imports
import schema from "./schema.sql?raw";
import "./index.css";

import { Sidebar } from "@/components/layout/Sidebar";
import { Header } from "@/components/layout/Header";
import { IssueList } from "@/components/issues/IssueList";
import { IssueDetail } from "@/components/issues/IssueDetail";
import { IssueForm } from "@/components/issues/IssueForm";
import { FilterBar } from "@/components/filters/FilterBar";
import { useFakeData } from "@/hooks/useFakeData";

async function initWasm() {
  const module = await import("groove-wasm");
  await module.default();
  return module;
}

// Check for persistence preference in URL params or localStorage
function shouldUsePersistence(): boolean {
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.has('persist')) {
    return urlParams.get('persist') === 'true';
  }
  // Default to true for persistence
  return localStorage.getItem('groove_persist') !== 'false';
}

function App() {
  const db = useJazz();

  // UI state
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);
  const [showMyIssues, setShowMyIssues] = useState(false);
  const [selectedIssueId, setSelectedIssueId] = useState<string | null>(null);
  const [showIssueForm, setShowIssueForm] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const pageSize = 20;

  // Filters
  const [statusFilter, setStatusFilter] = useState<string | undefined>();
  const [priorityFilter, setPriorityFilter] = useState<string | undefined>();
  const [assigneeFilter, setAssigneeFilter] = useState<string | undefined>();
  const [labelFilter, setLabelFilter] = useState<string | undefined>();

  // Current user (from fake data generation)
  const { initialized, currentUserId } = useFakeData(db);

  // Compute effective assignee filter (explicit filter takes precedence over "My Issues")
  const effectiveAssignee = assigneeFilter || (showMyIssues ? currentUserId : undefined);

  // Subscribe to filtered issues - no useMemo needed, hook handles structural equality
  // undefined values are automatically ignored by the where clause builder
  const [filteredIssues, issuesLoading] = useAll(
    app.issues
      .where({
        project: selectedProjectId ?? undefined,
        status: statusFilter,
        priority: priorityFilter,
        IssueAssignees: effectiveAssignee ? { some: { user: effectiveAssignee } } : undefined,
        IssueLabels: labelFilter ? { some: { label: labelFilter } } : undefined,
      })
      .with({
        project: true,
        IssueLabels: { label: true },
        IssueAssignees: { user: true },
      })
  );

  // Reset page when filters change
  useEffect(() => {
    setCurrentPage(0);
  }, [selectedProjectId, showMyIssues, statusFilter, priorityFilter, assigneeFilter, labelFilter]);

  if (!initialized || issuesLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-muted-foreground">Loading...</div>
      </div>
    );
  }

  return (
    <div className="flex h-screen">
      <Sidebar
        selectedProjectId={selectedProjectId}
        onSelectProject={setSelectedProjectId}
        showMyIssues={showMyIssues}
        onToggleMyIssues={setShowMyIssues}
      />

      <div className="flex flex-1 flex-col">
        <Header
          currentUserId={currentUserId}
          onCreateIssue={() => setShowIssueForm(true)}
        />

        <FilterBar
          statusFilter={statusFilter}
          onStatusFilterChange={setStatusFilter}
          priorityFilter={priorityFilter}
          onPriorityFilterChange={setPriorityFilter}
          assigneeFilter={assigneeFilter}
          onAssigneeFilterChange={setAssigneeFilter}
          labelFilter={labelFilter}
          onLabelFilterChange={setLabelFilter}
        />

        <IssueList
          issues={filteredIssues}
          onSelectIssue={(issue) => setSelectedIssueId(issue.id)}
          currentPage={currentPage}
          pageSize={pageSize}
          onNextPage={() => setCurrentPage((p) => p + 1)}
          onPrevPage={() => setCurrentPage((p) => Math.max(0, p - 1))}
        />
      </div>

      <IssueDetail
        issueId={selectedIssueId}
        open={!!selectedIssueId}
        onOpenChange={(open) => !open && setSelectedIssueId(null)}
      />

      <IssueForm
        open={showIssueForm}
        onOpenChange={setShowIssueForm}
      />
    </div>
  );
}

// Root component that sets up the database and provider
function Root() {
  const [wasmDb, setWasmDb] = useState<WasmDatabaseLike | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPersistent, setIsPersistent] = useState<boolean | null>(null);
  const initRef = useRef(false);

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    async function init() {
      try {
        const wasm = await initWasm();
        const usePersistence = shouldUsePersistence();
        setIsPersistent(usePersistence);

        let db;
        if (usePersistence) {
          // Use IndexedDB for persistence
          console.log("Using IndexedDB persistence...");
          db = await wasm.WasmDatabase.withIndexedDb(undefined);

          // Check if this is a fresh database (no tables)
          const tables = db.list_tables() as string[];
          const needsSchema = !tables || tables.length === 0;

          if (needsSchema) {
            console.log("Initializing schema...");
            db.init_schema(schema);
          } else {
            console.log("Loaded existing database from IndexedDB with tables:", tables);
          }
        } else {
          // Use in-memory database
          console.log("Using in-memory database (no persistence)");
          db = new wasm.WasmDatabase();
          db.init_schema(schema);
        }

        setWasmDb(db as unknown as WasmDatabaseLike);
      } catch (e) {
        console.error("Init error:", e);
        setError(e instanceof Error ? e.message : String(e));
      }
    }
    init();
  }, []);

  if (error) {
    return (
      <div className="flex h-screen items-center justify-center text-destructive">
        Error: {error}
      </div>
    );
  }

  if (!wasmDb) {
    return (
      <div className="flex h-screen flex-col items-center justify-center gap-2">
        <div className="text-muted-foreground">Initializing WASM...</div>
        {isPersistent !== null && (
          <div className="text-xs text-muted-foreground">
            Storage: {isPersistent ? "IndexedDB (persistent)" : "Memory (not persistent)"}
          </div>
        )}
      </div>
    );
  }

  return (
    <JazzProvider database={wasmDb}>
      <App />
    </JazzProvider>
  );
}

export default Root;
