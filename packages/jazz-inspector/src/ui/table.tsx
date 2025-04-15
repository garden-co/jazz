import { styled } from "goober";
import React from "react";

const StyledTable = styled("table")`
  width: 100%;
`;

const StyledThead = styled("thead")`
  text-align: left;
  border-bottom: 1px solid var(--j-border-color);
  background-color: var(--j-neutral-100);

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-neutral-925);
  }
`;

const StyledTbody = styled("tbody")`
  tr {
    border-bottom: 1px solid var(--j-border-color);

    &:last-child {
      border-bottom: none;
    }
  }
`;

const StyledTh = styled("th")`
  font-weight: 500;
  padding: 0.5rem 0.75rem;
  color: var(--j-text-color-strong);
`;

const StyledTd = styled("td")`
  padding: 0.5rem 0.75rem;
`;

export const Table = React.forwardRef<
  HTMLTableElement,
  React.HTMLAttributes<HTMLTableElement>
>(({ children, ...props }, ref) => (
  <StyledTable ref={ref} {...props}>
    {children}
  </StyledTable>
));

export const TableHead = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ children, ...props }, ref) => (
  <StyledThead ref={ref} {...props}>
    {children}
  </StyledThead>
));

export const TableBody = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ children, ...props }, ref) => (
  <StyledTbody ref={ref} {...props}>
    {children}
  </StyledTbody>
));

export const TableRow = React.forwardRef<
  HTMLTableRowElement,
  React.HTMLAttributes<HTMLTableRowElement>
>(({ children, ...props }, ref) => (
  <tr ref={ref} {...props}>
    {children}
  </tr>
));

export const TableHeader = React.forwardRef<
  HTMLTableCellElement,
  React.ThHTMLAttributes<HTMLTableCellElement>
>(({ children, ...props }, ref) => (
  <StyledTh ref={ref} {...props}>
    {children}
  </StyledTh>
));

export const TableCell = React.forwardRef<
  HTMLTableCellElement,
  React.TdHTMLAttributes<HTMLTableCellElement>
>(({ children, ...props }, ref) => (
  <StyledTd ref={ref} {...props}>
    {children}
  </StyledTd>
));
