export type User = {
  userId: string;
  username: string;
  password: string;
  role: string;
};

// In-memory user store. In a real app, replace this with a database.
export const users = new Map<string, User>([
  [
    "admin@example.com",
    {
      userId: "admin",
      username: "admin@example.com",
      password: "admin",
      role: "admin",
    },
  ],
]);

let counter = 1;

/** Returns the user if credentials match, or null if not found / wrong password. */
export function findUser(email: string, password: string): User | null {
  const existing = users.get(email);
  if (!existing || existing.password !== password) return null;
  return existing;
}

/** Creates and returns a new user, or null if the email is already taken. */
export function createUser(email: string, password: string): User | null {
  if (users.has(email)) return null;
  const user: User = { userId: `user-${counter++}`, username: email, password, role: "member" };
  users.set(email, user);
  return user;
}
