import { useState, useEffect, useRef, useMemo } from "react";
import { JazzProvider, useJazz, useAll } from "@jazz/react";
import { createDatabase, type Database } from "./generated/client";
import type { Issue, IssueFilter } from "./generated/types";

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
  const [selectedIssue, setSelectedIssue] = useState<Issue | null>(null);
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

  // Subscribe to reference data (no filters)
  const { data: users, loading: usersLoading } = useAll(db.users, {});
  const { data: projects, loading: projectsLoading } = useAll(db.projects, {});
  const { data: labels, loading: labelsLoading } = useAll(db.labels, {});

  // Subscribe to all issues for sidebar counts
  const { data: allIssues } = useAll(db.issues, {});

  // Subscribe to filtered issues based on UI state (all filtering via SQL JOINs!)
  const { data: filteredIssues, loading: issuesLoading } = useAll(db.issues, { where: issuesFilter });

  // Subscribe to all junction tables for UI display (showing labels/assignees on issues)
  const { data: allIssueLabels } = useAll(db.issuelabels, {});
  const { data: allIssueAssignees } = useAll(db.issueassignees, {});

  const currentUser = useMemo(() => {
    return users.find((u) => u.id === currentUserId) || null;
  }, [users, currentUserId]);

  // Get data for selected issue (use allIssueAssignees/allIssueLabels for complete data)
  const selectedIssueData = useMemo(() => {
    if (!selectedIssue) return { project: undefined, assignees: [], labels: [] };

    const project = projects.find((p) => p.id === selectedIssue.project);
    const assigneeIds = allIssueAssignees
      .filter((ia) => ia.issue === selectedIssue.id)
      .map((ia) => ia.user);
    const assignees = users.filter((u) => assigneeIds.includes(u.id));
    const labelIds = allIssueLabels
      .filter((il) => il.issue === selectedIssue.id)
      .map((il) => il.label);
    const issueLabelsData = labels.filter((l) => labelIds.includes(l.id));

    return { project, assignees, labels: issueLabelsData };
  }, [selectedIssue, projects, users, labels, allIssueAssignees, allIssueLabels]);

  // Reset page when filters change
  useEffect(() => {
    setCurrentPage(0);
  }, [selectedProjectId, showMyIssues, statusFilter, priorityFilter, assigneeFilter, labelFilter]);

  const isLoading = usersLoading || projectsLoading || issuesLoading || labelsLoading || !initialized;

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-muted-foreground">Loading...</div>
      </div>
    );
  }

  return (
    <div className="flex h-screen">
      <Sidebar
        projects={projects}
        issues={allIssues}
        selectedProjectId={selectedProjectId}
        onSelectProject={setSelectedProjectId}
        showMyIssues={showMyIssues}
        onToggleMyIssues={setShowMyIssues}
        currentUserId={currentUserId}
      />

      <div className="flex flex-1 flex-col">
        <Header
          currentUser={currentUser}
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
          users={users}
          labels={labels}
        />

        <IssueList
          issues={filteredIssues}
          projects={projects}
          users={users}
          labels={labels}
          issueLabels={allIssueLabels}
          issueAssignees={allIssueAssignees}
          onSelectIssue={setSelectedIssue}
          currentPage={currentPage}
          pageSize={pageSize}
          onNextPage={() => setCurrentPage((p) => p + 1)}
          onPrevPage={() => setCurrentPage((p) => Math.max(0, p - 1))}
        />
      </div>

      <IssueDetail
        issue={selectedIssue}
        project={selectedIssueData.project}
        assignees={selectedIssueData.assignees}
        labels={selectedIssueData.labels}
        allUsers={users}
        allLabels={labels}
        allProjects={projects}
        db={db}
        open={!!selectedIssue}
        onOpenChange={(open) => !open && setSelectedIssue(null)}
      />

      <IssueForm
        allUsers={users}
        allLabels={labels}
        allProjects={projects}
        db={db}
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
