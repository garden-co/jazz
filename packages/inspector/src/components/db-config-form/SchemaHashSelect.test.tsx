import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SchemaHashSelect } from "./SchemaHashSelect";

describe("SchemaHashSelect", () => {
  afterEach(() => {
    cleanup();
  });

  it("shows short schema hashes with upload time while submitting full hashes", () => {
    const onSelect = vi.fn();
    const hash = "aaaaaaaaaaaabbbbbbbbbbbbccccccccccccddddddddddddeeeeeeeeeeeeffff";

    render(
      <SchemaHashSelect
        schemas={[{ hash, publishedAt: Date.UTC(2026, 5, 18, 19, 15) }]}
        onSelect={onSelect}
      />,
    );

    expect(screen.getByRole("option", { name: /aaaaaaaaaaaa - uploaded / })).not.toBeNull();

    fireEvent.change(screen.getByLabelText("Schema hash"), { target: { value: hash } });
    fireEvent.click(screen.getByRole("button", { name: "Use schema" }));

    expect(onSelect).toHaveBeenCalledWith(hash);
  });
});
