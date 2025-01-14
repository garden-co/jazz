import { render, screen } from "@testing-library/react";
import { JazzTestProvider, createJazzTestAccount } from "jazz-react/testing";
import { ID } from "jazz-tools";
import React from "react";
import { describe, expect, it } from "vitest";
import { DocumentComponent } from "./index.js";
import { Document } from "./schema.js";

describe("Document", () => {
  it("renders without crashing", async () => {
    const account = await createJazzTestAccount();
    render(
      <JazzTestProvider account={account}>
        <DocumentComponent docID={"test-doc-id" as ID<Document>} />
      </JazzTestProvider>,
    );

    expect(screen.getByTestId("editor")).toBeInTheDocument();
  });
});
