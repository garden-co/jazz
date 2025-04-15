import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { styled } from "goober";
import React from "react";
import { Badge } from "../ui/badge.js";
import { Heading } from "../ui/heading.js";
import { Text } from "../ui/text.js";
import { CoStreamView } from "./co-stream-view.js";
import { GridView } from "./grid-view.js";
import { GroupView } from "./group-view.js";
import { TableView } from "./table-viewer.js";
import { TypeIcon } from "./type-icon.js";
import { PageInfo } from "./types.js";
import { resolveCoValue, useResolvedCoValue } from "./use-resolve-covalue.js";
import { AccountOrGroupPreview } from "./value-renderer.js";

interface PageContainerProps extends React.HTMLAttributes<HTMLDivElement> {
  isTopLevel?: boolean;
}

const BasePageContainer = React.forwardRef<HTMLDivElement, PageContainerProps>(
  ({ isTopLevel, ...rest }, ref) => <div ref={ref} {...rest} />,
);

const PageContainer = styled(BasePageContainer)<PageContainerProps>`
  position: absolute;
  z-index: 10;
  inset: 0;
  width: 100%;
  height: 100%;
  padding: 0 0.75rem;
`;

const BackButton = styled("div")`
  position: absolute;
  left: 0;
  right: 0;
  top: 0;
  height: 2.5rem;
`;

const HeaderContainer = styled("div")`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
`;

const TitleContainer = styled("div")`
  display: flex;
  align-items: center;
  gap: 0.75rem;
`;

const Title = styled(Heading)`
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.25rem;
`;

const BadgeContainer = styled("div")`
  display: flex;
  align-items: center;
  gap: 0.75rem;
`;

const ContentContainer = styled("div")`
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding-bottom: 2rem;
`;

type PageProps = {
  coId: CoID<RawCoValue>;
  node: LocalNode;
  name: string;
  onNavigate: (newPages: PageInfo[]) => void;
  onHeaderClick?: () => void;
  isTopLevel?: boolean;
  style?: React.CSSProperties;
  className?: string;
};

function View(
  props: PageProps & { coValue: Awaited<ReturnType<typeof resolveCoValue>> },
) {
  const { type, extendedType } = props.coValue;
  const { snapshot, value } = props.coValue;
  const { node, onNavigate } = props;

  if (!snapshot || snapshot === "unavailable") return;

  if (type === "costream") {
    return (
      <CoStreamView
        data={snapshot}
        onNavigate={onNavigate}
        node={node}
        value={value as RawCoStream}
      />
    );
  }

  if (extendedType === "group") {
    return <GroupView data={snapshot} node={node} onNavigate={onNavigate} />;
  }

  if (type === "colist" || extendedType === "record") {
    return <TableView data={snapshot} node={node} onNavigate={onNavigate} />;
  }

  return <GridView data={snapshot} onNavigate={onNavigate} node={node} />;
}

export function Page(props: PageProps) {
  const {
    coId,
    node,
    name,
    onNavigate,
    onHeaderClick,
    style,
    className = "",
    isTopLevel,
  } = props;
  const coValue = useResolvedCoValue(coId, node);

  const { value, snapshot, type, extendedType } = coValue;

  if (snapshot === "unavailable") {
    return <div style={style}>Data unavailable</div>;
  }

  if (!snapshot) {
    return <div style={style}></div>;
  }

  return (
    <PageContainer style={style} className={className} isTopLevel={isTopLevel}>
      {!isTopLevel && (
        <BackButton
          aria-label="Back"
          onClick={() => {
            onHeaderClick?.();
          }}
          aria-hidden="true"
        ></BackButton>
      )}
      <HeaderContainer>
        <TitleContainer>
          <Title>
            <span>
              {name}
              {typeof snapshot === "object" && "name" in snapshot ? (
                <span style={{ color: "#57534e", fontWeight: 500 }}>
                  {" "}
                  {(snapshot as { name: string }).name}
                </span>
              ) : null}
            </span>
          </Title>
          <BadgeContainer>
            <Badge>
              {type && <TypeIcon type={type} extendedType={extendedType} />}
            </Badge>
            <Badge>{coId}</Badge>
          </BadgeContainer>
        </TitleContainer>
      </HeaderContainer>
      <ContentContainer>
        <View {...props} coValue={coValue} />
        {extendedType !== "account" && extendedType !== "group" && (
          <Text muted>
            Owned by{" "}
            <AccountOrGroupPreview
              coId={value.group.id}
              node={node}
              showId
              onClick={() => {
                onNavigate([{ coId: value.group.id, name: "owner" }]);
              }}
            />
          </Text>
        )}
      </ContentContainer>
    </PageContainer>
  );
}
