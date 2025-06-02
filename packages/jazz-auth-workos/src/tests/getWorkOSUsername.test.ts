import { describe, expect, it } from 'vitest'
import { getWorkOSUsername } from '../getWorkOSUsername.js'
import type { WorkOSAuthHook } from "../types.js"

describe("getWorkOSUsername", () => {
    it("should return null if no user", () => {
        const mockWorkOS = {
            user: null
        } as WorkOSAuthHook;

        expect(getWorkOSUsername(mockWorkOS)).toBe(null);
    })

    it("should return firstName + lastName if available", () => {
        const mockClerk = {
          user: {
            firstName: "John",
            lastName: "Doe",
            email: "johndoe@example.com",
          },
        } as WorkOSAuthHook;
    
        expect(getWorkOSUsername(mockClerk)).toBe("John Doe");
    });

    it("should return firstName if available and no lastName or fullName", () => {
        const mockClerk = {
          user: {
            firstName: "John",
            email: "johndoe@example.com",
          },
        } as WorkOSAuthHook;
    
        expect(getWorkOSUsername(mockClerk)).toBe("John");
    });

    it("should return username if available and no names", () => {
        const mockClerk = {
          user: {
            email: "johndoe@example.com",
          },
        } as WorkOSAuthHook;
    
        expect(getWorkOSUsername(mockClerk)).toBe("johndoe");
    });

    it("should return user id as last resort", () => {
        const mockClerk = {
            user: {
                id: "user_123",
            },
        } as WorkOSAuthHook;

        expect(getWorkOSUsername(mockClerk)).toBe("user_123");
    });

})