import { useState, useEffect, useRef, useMemo } from "react";
import { JazzProvider, useJazz, useAll } from "@jazz/react";
import { createDatabase, type Database } from "./generated/client";
import type { Issue } from "./generated/types";

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

  // Subscribe to all data
  const { data: users, loading: usersLoading } = useAll(db.users, {});
  const { data: projects, loading: projectsLoading } = useAll(db.projects, {});
  const { data: issues, loading: issuesLoading } = useAll(db.issues, {});
  const { data: labels, loading: labelsLoading } = useAll(db.labels, {});
  const { data: issueLabels } = useAll(db.issuelabels, {});
  const { data: issueAssignees } = useAll(db.issueassignees, {});

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

  const currentUser = useMemo(() => {
    return users.find((u) => u.id === currentUserId) || null;
  }, [users, currentUserId]);

  // Filter issues
  const filteredIssues = useMemo(() => {
    let filtered = [...issues];

    // Filter by project
    if (selectedProjectId) {
      filtered = filtered.filter((i) => i.project === selectedProjectId);
    }

    // Filter by "my issues" (issues assigned to current user)
    if (showMyIssues && currentUserId) {
      const myIssueIds = new Set(
        issueAssignees.filter((ia) => ia.user === currentUserId).map((ia) => ia.issue)
      );
      filtered = filtered.filter((i) => myIssueIds.has(i.id));
    }

    // Apply additional filters
    if (statusFilter) {
      filtered = filtered.filter((i) => i.status === statusFilter);
    }
    if (priorityFilter) {
      filtered = filtered.filter((i) => i.priority === priorityFilter);
    }
    if (assigneeFilter) {
      const assignedIssueIds = new Set(
        issueAssignees.filter((ia) => ia.user === assigneeFilter).map((ia) => ia.issue)
      );
      filtered = filtered.filter((i) => assignedIssueIds.has(i.id));
    }
    if (labelFilter) {
      const labeledIssueIds = new Set(
        issueLabels.filter((il) => il.label === labelFilter).map((il) => il.issue)
      );
      filtered = filtered.filter((i) => labeledIssueIds.has(i.id));
    }

    return filtered;
  }, [
    issues,
    selectedProjectId,
    showMyIssues,
    currentUserId,
    statusFilter,
    priorityFilter,
    assigneeFilter,
    labelFilter,
    issueAssignees,
    issueLabels,
  ]);

  // Get data for selected issue
  const selectedIssueData = useMemo(() => {
    if (!selectedIssue) return { project: undefined, assignees: [], labels: [] };

    const project = projects.find((p) => p.id === selectedIssue.project);
    const assigneeIds = issueAssignees
      .filter((ia) => ia.issue === selectedIssue.id)
      .map((ia) => ia.user);
    const assignees = users.filter((u) => assigneeIds.includes(u.id));
    const labelIds = issueLabels
      .filter((il) => il.issue === selectedIssue.id)
      .map((il) => il.label);
    const issueLabelsData = labels.filter((l) => labelIds.includes(l.id));

    return { project, assignees, labels: issueLabelsData };
  }, [selectedIssue, projects, users, labels, issueAssignees, issueLabels]);

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
        issues={issues}
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
          issueLabels={issueLabels}
          issueAssignees={issueAssignees}
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
