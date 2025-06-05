import { Framework } from "../framework";

export type DoneStatus =
  | number // represents percentage done
  | Partial<Record<Framework, number>>;

export type DocNavigationItem = {
  name: string;
  href: string;
  done: DoneStatus;
  framework?: Framework;
  next?: DocNavigationItem | null;
  previous?: DocNavigationItem | null;
  excludeFromNavigation?: boolean;
};

export type DocNavigationSection = {
  name: string;
  items: DocNavigationItem[];
  collapse?: boolean;
  prefix?: string;
};