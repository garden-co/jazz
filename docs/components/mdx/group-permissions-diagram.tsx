"use client";

import { useEffect, useRef, useState } from "react";
import {
  Check,
  ChevronDown,
  ChevronRight,
  Edit2,
  Eye,
  FileText,
  Folder,
  Pencil,
  Plus,
  Shield,
  Users,
  type LucideIcon,
} from "lucide-react";

import { cn } from "@/lib/cn";
import { DiagramFrame, DiagramStyles } from "./diagram";

type Role = "reader" | "contributor" | "writer" | "admin";
const ROLES: Role[] = ["reader", "contributor", "writer", "admin"];

const ROLE_META: Record<Role, { label: string; blurb: string; icon: LucideIcon }> = {
  reader: {
    label: "Reader",
    blurb: "See everything in the workspace.",
    icon: Eye,
  },
  contributor: {
    label: "Contributor",
    blurb: "Create files and edit the ones you own.",
    icon: Pencil,
  },
  writer: {
    label: "Writer",
    blurb: "Edit anything in the workspace.",
    icon: Edit2,
  },
  admin: {
    label: "Admin",
    blurb: "Edit anything and manage members.",
    icon: Shield,
  },
};

type Author = "you" | "alice" | "bob" | "carol";

const AUTHOR_LABEL: Record<Author, string> = {
  you: "You",
  alice: "Alice",
  bob: "Bob",
  carol: "Carol",
};

const AUTHOR_TINT: Record<Author, string> = {
  you: "bg-fd-primary/20 text-fd-primary ring-fd-primary/40",
  alice: "bg-rose-500/20 text-rose-700 dark:text-rose-300 ring-rose-500/40",
  bob: "bg-sky-500/20 text-sky-700 dark:text-sky-300 ring-sky-500/40",
  carol: "bg-emerald-500/20 text-emerald-700 dark:text-emerald-300 ring-emerald-500/40",
};

type Doc = { id: string; title: string; author: Author; edited: string };

const DOCS: Doc[] = [
  { id: "spec", title: "Spec draft", author: "alice", edited: "Today" },
  { id: "roadmap", title: "Q2 roadmap", author: "bob", edited: "Yesterday" },
  { id: "notes", title: "Design notes", author: "you", edited: "3 days ago" },
  { id: "okrs", title: "Team OKRs", author: "carol", edited: "Last week" },
];

function canCreate(r: Role) {
  return r !== "reader";
}
function canManage(r: Role) {
  return r === "admin";
}
function canEdit(r: Role, author: Author) {
  if (r === "writer" || r === "admin") return true;
  return r === "contributor" && author === "you";
}

export function GroupPermissionsDiagram() {
  const [role, setRole] = useState<Role | null>(null);
  return (
    <DiagramFrame
      eyebrow="Try it"
      description={
        <>
          Pick a role to enter the workspace. The same files appear for everyone, but the actions
          you can take change with your role — swap any time from the chip in the top-right.
        </>
      }
      responsive
    >
      <DiagramStyles />
      {role === null ? (
        <Onboarding onChoose={setRole} />
      ) : (
        <Workspace role={role} onChange={setRole} />
      )}
    </DiagramFrame>
  );
}

function Onboarding({ onChoose }: { onChoose: (r: Role) => void }) {
  return (
    <div key="onboarding">
      <h3 className="text-sm font-semibold text-fd-foreground mb-3 text-center">
        How would you like to enter this workspace?
      </h3>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-2xl mx-auto">
        {ROLES.map((r) => {
          const meta = ROLE_META[r];
          const Icon = meta.icon;
          return (
            <button
              key={r}
              type="button"
              onClick={() => onChoose(r)}
              className="group flex items-center gap-3 rounded-lg border border-fd-border bg-fd-card p-3 text-left hover:border-fd-primary/50 hover:bg-fd-accent transition-colors cursor-pointer"
            >
              <span className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-fd-muted text-fd-foreground">
                <Icon className="h-4 w-4" />
              </span>
              <div className="flex-1 min-w-0">
                <div className="text-sm font-semibold text-fd-foreground">{meta.label}</div>
                <div className="text-xs text-fd-muted-foreground">{meta.blurb}</div>
              </div>
              <ChevronRight className="h-4 w-4 text-fd-muted-foreground group-hover:text-fd-foreground transition-colors shrink-0" />
            </button>
          );
        })}
      </div>
    </div>
  );
}

function Workspace({ role, onChange }: { role: Role; onChange: (r: Role) => void }) {
  const [shown, setShown] = useState(false);
  const [highlightChip, setHighlightChip] = useState(true);
  useEffect(() => {
    const raf = requestAnimationFrame(() => setShown(true));
    const t = setTimeout(() => setHighlightChip(false), 1600);
    return () => {
      cancelAnimationFrame(raf);
      clearTimeout(t);
    };
  }, []);
  return (
    <div
      key="workspace"
      className={cn("transition-opacity duration-300", shown ? "opacity-100" : "opacity-0")}
    >
      <div className="rounded-lg border border-fd-border bg-fd-card overflow-hidden">
        <header className="flex items-center justify-between gap-2 border-b border-fd-border px-3 py-2">
          <div className="flex items-center gap-2 min-w-0">
            <Folder className="h-4 w-4 text-fd-muted-foreground shrink-0" />
            <span className="text-sm font-semibold text-fd-foreground truncate">Product team</span>
          </div>
          <div className="flex items-center gap-1.5">
            <HeaderButton
              icon={Users}
              label="Members"
              shown={canManage(role)}
              disabled={!canManage(role)}
            />
            <HeaderButton icon={Plus} label="New" shown disabled={!canCreate(role)} />
            <RoleChip role={role} onChange={onChange} highlight={highlightChip} />
          </div>
        </header>
        <ul className="flex flex-col m-0 p-0 list-none">
          {DOCS.map((d, i) => (
            <DocRow
              key={d.id}
              doc={d}
              editable={canEdit(role, d.author)}
              isLast={i === DOCS.length - 1}
            />
          ))}
        </ul>
      </div>
    </div>
  );
}

function HeaderButton({
  icon: Icon,
  label,
  shown,
  disabled,
}: {
  icon: LucideIcon;
  label: string;
  shown: boolean;
  disabled: boolean;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      aria-hidden={!shown}
      className={cn(
        "inline-flex items-center gap-1 rounded border px-2 py-1 text-[11px] font-medium transition-all duration-300",
        shown
          ? disabled
            ? "border-dashed border-fd-border text-fd-muted-foreground opacity-40 cursor-not-allowed"
            : "border-fd-border bg-fd-card text-fd-foreground hover:bg-fd-accent cursor-pointer opacity-100"
          : "opacity-0 pointer-events-none w-0 px-0 border-transparent overflow-hidden",
      )}
    >
      <Icon className="h-3 w-3" />
      <span>{label}</span>
    </button>
  );
}

function DocRow({ doc, editable, isLast }: { doc: Doc; editable: boolean; isLast: boolean }) {
  return (
    <li
      className={cn(
        "flex items-center gap-2 px-3 py-2 transition-colors hover:bg-fd-accent/50",
        !isLast && "border-b border-fd-border",
      )}
    >
      <FileText className="h-4 w-4 text-fd-muted-foreground shrink-0" />
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium text-fd-foreground truncate">{doc.title}</div>
        <div className="flex items-center gap-1.5 text-[11px] text-fd-muted-foreground mt-0.5">
          <span
            className={cn(
              "inline-flex h-4 w-4 items-center justify-center rounded-full text-[9px] font-semibold ring-1",
              AUTHOR_TINT[doc.author],
            )}
            aria-hidden
          >
            {AUTHOR_LABEL[doc.author][0]}
          </span>
          <span>{AUTHOR_LABEL[doc.author]}</span>
          <span aria-hidden>·</span>
          <span>{doc.edited}</span>
        </div>
      </div>
      <button
        type="button"
        disabled={!editable}
        aria-label={editable ? `Edit ${doc.title}` : `Cannot edit ${doc.title}`}
        title={editable ? "Edit" : "Your role doesn't allow editing this file"}
        className={cn(
          "flex h-7 w-7 items-center justify-center rounded transition-all duration-300 shrink-0",
          editable
            ? "text-fd-foreground hover:bg-fd-accent cursor-pointer opacity-100"
            : "text-fd-muted-foreground opacity-30 cursor-not-allowed",
        )}
      >
        <Pencil className="h-3.5 w-3.5" />
      </button>
    </li>
  );
}

function RoleChip({
  role,
  onChange,
  highlight,
}: {
  role: Role;
  onChange: (r: Role) => void;
  highlight?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const meta = ROLE_META[role];
  const Icon = meta.icon;

  return (
    <div ref={ref} className="relative">
      {highlight && (
        <span
          className="diagram-pulse"
          style={{
            position: "absolute",
            inset: "-2px",
            borderRadius: "0.375rem",
            pointerEvents: "none",
          }}
        />
      )}
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-haspopup="listbox"
        aria-expanded={open}
        className="inline-flex items-center gap-1 rounded border border-fd-border bg-fd-card px-2 py-1 text-[11px] font-medium text-fd-foreground hover:bg-fd-accent cursor-pointer transition-colors"
      >
        <Icon className="h-3 w-3" />
        <span>{meta.label}</span>
        <ChevronDown className={cn("h-3 w-3 transition-transform", open && "rotate-180")} />
      </button>
      {open && (
        <div
          role="listbox"
          className="absolute right-0 top-full mt-1 z-20 min-w-[10rem] rounded-md border border-fd-border bg-fd-card shadow-lg overflow-hidden"
        >
          {ROLES.map((r) => {
            const m = ROLE_META[r];
            const RoleIcon = m.icon;
            const active = r === role;
            return (
              <button
                key={r}
                type="button"
                role="option"
                aria-selected={active}
                onClick={() => {
                  onChange(r);
                  setOpen(false);
                }}
                className={cn(
                  "flex w-full items-center gap-2 px-2 py-1.5 text-[11px] font-medium text-left hover:bg-fd-accent cursor-pointer transition-colors",
                  active && "bg-fd-accent/50",
                )}
              >
                <RoleIcon className="h-3 w-3 shrink-0" />
                <span className="flex-1">{m.label}</span>
                {active && <Check className="h-3 w-3 text-fd-primary" />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
