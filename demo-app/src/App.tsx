import { useState, useEffect, useRef, useMemo } from "react";
import { JazzProvider, useJazz, useAll } from "@jazz/react";
import { createDatabase, type Database } from "./generated/client";
import type { IssueFilter, IssueLoaded, IssueIncludes } from "./generated/types";

// Define the include spec for issues with all related data
const issueIncludes = {
  project: true,
  IssueLabels: { label: true },
  IssueAssignees: { user: true },
} as const satisfies IssueIncludes;

// Type for an issue with all includes loaded
export type LoadedIssue = IssueLoaded<typeof issueIncludes>;

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

function App() {
  const db = useJazz() as unknown as Database;

  // UI state
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);
  const [showMyIssues, setShowMyIssues] = useState(false);
  const [selectedIssue, setSelectedIssue] = useState<LoadedIssue | null>(null);
  const [showIssueForm, setShowIssueForm] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const pageSize = 20;

  // Filters
  const [statusFilter, setStatusFilter] = useState<string | null>(null);
  const [priorityFilter, setPriorityFilter] = useState<string | null>(null);
  const [assigneeFilter, setAssigneeFilter] = useState<string | null>(null);
  const [labelFilter, setLabelFilter] = useState<string | null>(null);

  // Current user (from fake data generation)
  const { initialized, currentUserId } = useFakeData(db);

  // Build issues filter from UI state (SQL-based filtering with relation filters)
  const issuesFilter = useMemo((): IssueFilter | undefined => {
    const filter: IssueFilter = {};

    // Direct column filters
    if (selectedProjectId) {
      filter.project = selectedProjectId;
    }
    if (statusFilter) {
      filter.status = statusFilter;
    }
    if (priorityFilter) {
      filter.priority = priorityFilter;
    }

    // Relation filter: assignee (via IssueAssignees junction table)
    const userToFilter = assigneeFilter || (showMyIssues ? currentUserId : null);
    if (userToFilter) {
      filter.IssueAssignees = { some: { user: userToFilter } };
    }

    // Relation filter: label (via IssueLabels junction table)
    if (labelFilter) {
      filter.IssueLabels = { some: { label: labelFilter } };
    }

    // Return undefined if no filters
    if (Object.keys(filter).length === 0) return undefined;
    return filter;
  }, [selectedProjectId, statusFilter, priorityFilter, assigneeFilter, showMyIssues, currentUserId, labelFilter]);

  // Build the query with filters and includes
  const issuesQuery = useMemo(() => {
    if (issuesFilter) {
      return db.issues.where(issuesFilter).with(issueIncludes);
    }
    return db.issues.with(issueIncludes);
  }, [db.issues, issuesFilter]);

  // Subscribe to filtered issues with all related data included
  const { data: filteredIssues, loading: issuesLoading } = useAll(issuesQuery);

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
          onSelectIssue={setSelectedIssue}
          currentPage={currentPage}
          pageSize={pageSize}
          onNextPage={() => setCurrentPage((p) => p + 1)}
          onPrevPage={() => setCurrentPage((p) => Math.max(0, p - 1))}
        />
      </div>

      <IssueDetail
        issue={selectedIssue}
        open={!!selectedIssue}
        onOpenChange={(open) => !open && setSelectedIssue(null)}
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
  const [db, setDb] = useState<Database | null>(null);
  const [error, setError] = useState<string | null>(null);
  const initRef = useRef(false);

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    async function init() {
      try {
        const wasm = await initWasm();
        const wasmDb = new wasm.WasmDatabase();

        // Initialize schema from imported SQL file
        wasmDb.init_schema(schema);

        const database = createDatabase(wasmDb);
        setDb(database);
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

  if (!db) {
    return (
      <div className="flex h-screen items-center justify-center text-muted-foreground">
        Initializing WASM...
      </div>
    );
  }

  return (
    <JazzProvider database={db as unknown as Parameters<typeof JazzProvider>[0]['database']}>
      <App />
    </JazzProvider>
  );
}

export default Root;
