import { styled } from "goober";

const GridThreeColumns = styled("div")`
  display: grid;
  grid-template-columns: repeat(1, minmax(0, 1fr));
  gap: 1rem;
  
  @media (min-width: 768px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  
  @media (min-width: 1280px) {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
`;

const GridTwoColumns = styled("div")`
  display: grid;
  grid-template-columns: repeat(1, minmax(0, 1fr));
  gap: 1rem;
  
  @media (min-width: 768px) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
`;

const GridOneColumn = styled("div")`
  display: grid;
  grid-template-columns: repeat(1, minmax(0, 1fr));
  gap: 1rem;
`;

export function Grid(
  props: React.HTMLAttributes<HTMLElement> & { cols: 1 | 2 | 3 },
) {
  const { cols, children, ...rest } = props;

  const Element = [GridOneColumn, GridTwoColumns, GridThreeColumns][cols - 1];

  return <Element {...rest}>{children}</Element>;
}
