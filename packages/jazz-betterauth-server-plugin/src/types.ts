import type { User } from "better-auth";

export interface UserWithJazz extends User {
  encryptedCredentials: string;
  salt: string;
}
