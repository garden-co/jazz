import { CoID, RawCoValue } from "cojson";
import { styled } from "goober";
import React, { type PropsWithChildren, useState } from "react";
import { Button } from "../ui/button.js";
import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { DeleteLocalData } from "./delete-local-data.js";
import { useRouter } from "../router/context.js";

export function Header({
  showDeleteLocalData = false,
  showClose = false,
  onClose,
  children,
}: PropsWithChildren<{
  showDeleteLocalData?: boolean;
  showClose?: boolean;
  onClose?: () => void;
}>) {
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, setPage } = useRouter();

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
    setCoValueId("");
  };

  return (
    <HeaderContainer>
      <Breadcrumbs />
      {path.length !== 0 && (
        <Form onSubmit={handleCoValueIdSubmit}>
          <Input
            label="CoValue ID"
            style={{ fontFamily: "monospace" }}
            hideLabel
            placeholder="co_z1234567890abcdef123456789"
            value={coValueId}
            onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
          />
        </Form>
      )}
      {children}
      {showDeleteLocalData && <DeleteLocalData />}
      {showClose && (
        <Button variant="plain" type="button" onClick={onClose}>
          Close
        </Button>
      )}
    </HeaderContainer>
  );
}

const HeaderContainer = styled("div")`
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0 0.75rem;
  margin: 0.75rem 0;
`;

const Form = styled("form")`
  width: 24rem;
`;
