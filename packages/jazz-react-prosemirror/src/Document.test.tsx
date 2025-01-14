import { render, screen } from "@testing-library/react";
import { Document } from "jazz-browser-prosemirror";
import { JazzTestProvider, createJazzTestAccount } from "jazz-react/testing";
import { ID } from "jazz-tools";
import * as React from "react";
import { describe, expect, it } from "vitest";
import { DocumentComponent } from "./index.js";

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
