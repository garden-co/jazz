import type { Db } from "jazz-tools";
import { app, MAX_FOLDER_DEPTH, type FolderRole } from "../schema.js";

export interface Folder {
  id: string;
  name: string;
  owner_id: string;
  parent_id?: string | null;
}

export interface Membership {
  folder_id: string;
  role: FolderRole;
}

export class FolderIndex {
  readonly byId = new Map<string, Folder>();
  readonly children = new Map<string, Folder[]>();
  readonly roots: Folder[] = [];

  constructor(
    folders: readonly Folder[],
    private readonly userId: string | undefined,
    private readonly memberships: readonly Membership[],
  ) {
    for (const folder of folders) this.byId.set(folder.id, folder);
    const byName = (a: Folder, b: Folder) => a.name.localeCompare(b.name);
    for (const folder of [...folders].sort(byName)) {
      // A shared subfolder whose parent is not visible is a root for this user.
      const parent = folder.parent_id ? this.byId.get(folder.parent_id) : undefined;
      if (!parent) this.roots.push(folder);
      else this.children.set(parent.id, [...(this.children.get(parent.id) ?? []), folder]);
    }
  }

  /** Root first, ending with the folder itself. */
  path(folderId: string | undefined): Folder[] {
    const path: Folder[] = [];
    let folder = folderId ? this.byId.get(folderId) : undefined;
    while (folder && path.length <= MAX_FOLDER_DEPTH + 1) {
      path.unshift(folder);
      folder = folder.parent_id ? this.byId.get(folder.parent_id) : undefined;
    }
    return path;
  }

  /** Depth of a folder below its top-level ancestor, starting at 0. */
  depth(folderId: string): number {
    return this.path(folderId).length - 1;
  }

  descendants(folderId: string): Folder[] {
    const result: Folder[] = [];
    const visit = (id: string) => {
      for (const child of this.children.get(id) ?? []) {
        result.push(child);
        visit(child.id);
      }
    };
    visit(folderId);
    return result;
  }

  isMine(folder: Folder): boolean {
    return folder.owner_id === this.userId;
  }

  /** Mirrors the edit rule in permissions.ts, walking up the visible tree. */
  canEdit(folderId: string | undefined): boolean {
    return this.path(folderId).some(
      (folder) =>
        folder.owner_id === this.userId ||
        this.memberships.some((m) => m.folder_id === folder.id && m.role === "editor"),
    );
  }

  roleIn(folderId: string | undefined): "owner" | FolderRole | undefined {
    const path = this.path(folderId);
    if (path.some((folder) => folder.owner_id === this.userId)) return "owner";
    if (this.canEdit(folderId)) return "editor";
    return path.length > 0 ? "viewer" : undefined;
  }

  /** Levels of subfolders below a folder, 0 for a leaf. */
  height(folderId: string): number {
    const children = this.children.get(folderId) ?? [];
    return children.reduce((max, child) => Math.max(max, 1 + this.height(child.id)), 0);
  }

  /**
   * Folders a file or folder may be moved into: editable, not the folder
   * itself or one of its subfolders, and shallow enough that inherited access
   * still reaches the moved subtree.
   */
  moveTargets(movingFolderId?: string): Folder[] {
    const excluded = new Set<string>();
    let height = -1;
    if (movingFolderId) {
      excluded.add(movingFolderId);
      for (const folder of this.descendants(movingFolderId)) excluded.add(folder.id);
      height = this.height(movingFolderId);
    }
    return [...this.byId.values()]
      .filter((folder) => !excluded.has(folder.id) && this.canEdit(folder.id))
      .filter((folder) => this.depth(folder.id) + 1 + height <= MAX_FOLDER_DEPTH)
      .sort((a, b) => this.label(a).localeCompare(this.label(b)));
  }

  label(folder: Folder): string {
    return this.path(folder.id)
      .map((f) => f.name)
      .join(" / ");
  }
}

/** Delete a folder with its subfolders and files. Files go first, leaves before parents. */
export async function deleteFolderTree(db: Db, index: FolderIndex, folderId: string) {
  const folders = [index.byId.get(folderId)!, ...index.descendants(folderId)];
  const ids = folders.map((folder) => folder.id);
  const files = await db.all(app.files.where({ folder_id: { in: ids } }).select("id"));
  for (const file of files) db.delete(app.files, file.id);
  for (const folder of folders.reverse()) db.delete(app.folders, folder.id);
}
