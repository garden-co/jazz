import { styled } from "goober";
export const Card = styled("div")`
  background-color: var(--j-background);
  border-radius: var(--j-radius-lg);
  box-shadow: var(--j-shadow-sm);
  border: 1px solid var(--j-border-color);
  padding: 1rem;
  text-align: left;
  overflow-x: hidden;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
`;

export const CardHeader = styled("div")`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const CardBody = styled("div")`
  flex: 1;
`;
