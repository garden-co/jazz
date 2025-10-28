import { beforeEach, describe, expect, it } from "vitest";
import { LinkedList } from "../queue/LinkedList.js";

describe("LinkedList", () => {
  let list: LinkedList<number>;

  beforeEach(() => {
    list = new LinkedList<number>();
  });

  describe("initialization", () => {
    it("should create an empty list", () => {
      expect(list.length).toBe(0);
      expect(list.head).toBeUndefined();
      expect(list.tail).toBeUndefined();
    });
  });

  describe("push", () => {
    it("should add an element to an empty list", () => {
      list.push(1);
      expect(list.length).toBe(1);
      expect(list.head?.value).toBe(1);
      expect(list.tail?.value).toBe(1);
    });

    it("should add multiple elements in sequence", () => {
      list.push(1);
      list.push(2);
      list.push(3);
      expect(list.length).toBe(3);
      expect(list.head?.value).toBe(1);
      expect(list.tail?.value).toBe(3);
    });
  });

  describe("shift", () => {
    it("should return undefined for empty list", () => {
      expect(list.shift()).toBeUndefined();
      expect(list.length).toBe(0);
      expect(list.head).toBeUndefined();
      expect(list.tail).toBeUndefined();
    });

    it("should remove and return the first element", () => {
      list.push(1);
      list.push(2);

      const shifted = list.shift();
      expect(shifted).toBe(1);
      expect(list.length).toBe(1);
      expect(list.head?.value).toBe(2);
      expect(list.tail?.value).toBe(2);
    });

    it("should maintain correct order when shifting multiple times", () => {
      list.push(1);
      list.push(2);
      list.push(3);

      expect(list.shift()).toBe(1);
      expect(list.shift()).toBe(2);
      expect(list.shift()).toBe(3);
      expect(list.length).toBe(0);
      expect(list.head).toBeUndefined();
      expect(list.tail).toBeUndefined();
    });

    it("should handle shift after last element is removed", () => {
      list.push(1);
      list.shift();
      expect(list.shift()).toBeUndefined();
      expect(list.length).toBe(0);
      expect(list.head).toBeUndefined();
      expect(list.tail).toBeUndefined();
    });
  });

  describe("edge cases", () => {
    it("should handle push after all elements have been shifted", () => {
      list.push(1);
      list.shift();
      list.push(2);
      expect(list.length).toBe(1);
      expect(list.shift()).toBe(2);
    });

    it("should handle alternating push and shift operations", () => {
      list.push(1);
      expect(list.shift()).toBe(1);
      list.push(2);
      expect(list.shift()).toBe(2);
      expect(list.length).toBe(0);
    });
  });
});
