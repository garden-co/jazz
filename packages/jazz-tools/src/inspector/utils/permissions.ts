import { Role } from "cojson";

export function isWriter(role: Role | undefined): boolean {
  return (
    role === "writer" ||
    role === "admin" ||
    role === "manager" ||
    role === "writeOnly"
  );
}
