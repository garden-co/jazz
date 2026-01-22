import { styled } from "goober";
import { useNode } from "../contexts/node";
import { Heading } from "../ui/heading";
import { Button, Input } from "../ui";
import { useState } from "react";
import { CoID, RawCoValue } from "cojson";
import { useRouter } from "../router";
import { DeleteLocalData } from "../viewer/delete-local-data.js";

export interface HomePageProps {
  showDeleteLocalData?: boolean;
}

export function HomePage({ showDeleteLocalData }: HomePageProps) {
  const { localNode, accountID } = useNode();
  const { path, setPage } = useRouter();
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");

  if (!localNode || !accountID) {
    return <div>Loading...</div>;
  }

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
    setCoValueId("");
  };

  return (
    <PageContainer>
      <CenteredForm
        onSubmit={handleCoValueIdSubmit}
        aria-hidden={path.length !== 0}
      >
        <Heading>Jazz CoValue Inspector</Heading>

        <Input
          label="CoValue ID"
          className="font-mono"
          hideLabel
          placeholder="co_z1234567890abcdef123456789"
          value={coValueId}
          onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
        />

        <Button type="submit" variant="primary">
          Inspect CoValue
        </Button>

        <OrText>or</OrText>

        <Button
          variant="secondary"
          onClick={() => {
            setPage(accountID);
          }}
        >
          Inspect my account
        </Button>
      </CenteredForm>

      {showDeleteLocalData && (
        <BottomRight>
          <DeleteLocalData />
        </BottomRight>
      )}
    </PageContainer>
  );
}

const PageContainer = styled("div")`
  position: relative;
  height: 100%;
  width: 100%;
`;

const CenteredForm = styled("form")`
  display: flex;
  flex-direction: column;
  position: relative;
  top: -1.5rem;
  justify-content: center;
  gap: 0.5rem;
  height: 100%;
  width: 100%;
  max-width: 24rem;
  margin: 0 auto;
`;

const OrText = styled("p")`
  text-align: center;
`;

const BottomRight = styled("div")`
  position: absolute;
  bottom: 0.75rem;
  right: 0;
`;
