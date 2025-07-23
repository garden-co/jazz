import { CoID, LocalNode, RawCoStream, RawCoValue } from "cojson";
import { styled } from "goober";
import { type CSSProperties, type PropType, defineComponent } from "vue";
import { Badge } from "../ui/badge.js";
import { Heading } from "../ui/heading.js";
import { Text } from "../ui/text.js";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { AccountView } from "./account-view.js";
import { CoPlainTextView } from "./co-plain-text-view.js";
import { CoStreamView } from "./co-stream-view.js";
import { GridView } from "./grid-view.js";
import { GroupView } from "./group-view.js";
import { RoleDisplay } from "./role-display.js";
import { TableView } from "./table-viewer.js";
import { TypeIcon } from "./type-icon.js";
import { PageInfo } from "./types.js";
import { resolveCoValue, useResolvedCoValue } from "./use-resolve-covalue.js";

interface PageContainerProps {
  isTopLevel?: boolean;
  class?: string;
  style?: CSSProperties;
}

const BasePageContainer = (props: PageContainerProps) => (
  <div class={props.class} style={props.style} />
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

const Title = styled("h2")`
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.25rem;
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
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
  style?: CSSProperties;
  class?: string;
};

const View = defineComponent({
  name: "View",
  props: {
    coValue: {
      type: Object as PropType<Awaited<ReturnType<typeof resolveCoValue>>>,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode>,
      required: true,
    },
    onNavigate: {
      type: Function as PropType<(newPages: PageInfo[]) => void>,
      required: true,
    },
  },
  setup(props) {
    return () => {
      const { type, extendedType } = props.coValue;
      const { snapshot, value } = props.coValue;
      const { node, onNavigate } = props;

      if (!snapshot || snapshot === "unavailable") return null;

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
        return (
          <GroupView data={snapshot} node={node} onNavigate={onNavigate} />
        );
      }

      if (extendedType === "account") {
        return (
          <AccountView data={snapshot} node={node} onNavigate={onNavigate} />
        );
      }

      if (type === "coplaintext") {
        return <CoPlainTextView data={snapshot} />;
      }

      if (type === "colist" || extendedType === "record") {
        return (
          <TableView data={snapshot} node={node} onNavigate={onNavigate} />
        );
      }

      return <GridView data={snapshot} onNavigate={onNavigate} node={node} />;
    };
  },
});

export const Page = defineComponent({
  name: "Page",
  props: {
    coId: {
      type: String as any,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode>,
      required: true,
    },
    name: {
      type: String,
      required: true,
    },
    onNavigate: {
      type: Function as PropType<(newPages: PageInfo[]) => void>,
      required: true,
    },
    onHeaderClick: {
      type: Function as PropType<() => void>,
      required: false,
    },
    isTopLevel: {
      type: Boolean,
      default: false,
    },
    style: {
      type: Object as PropType<CSSProperties>,
      required: false,
    },
    class: {
      type: String,
      default: "",
    },
  },
  setup(props) {
    const coValue = useResolvedCoValue(props.coId, props.node);

    return () => {
      try {
        const { value, snapshot, type, extendedType } = coValue;

        if (!snapshot.value || snapshot.value === "unavailable") {
          return <div style={props.style}>Data unavailable</div>;
        }

        if (!value.value || !type.value) {
          return <div style={props.style}>Loading...</div>;
        }

        // Unwrap computed refs for component usage with error handling
        const unwrappedCoValue = {
          value: value.value,
          snapshot: snapshot.value,
          type: type.value,
          extendedType: extendedType.value,
        };

        return (
          <PageContainer
            style={props.style}
            class={props.class}
            isTopLevel={props.isTopLevel}
          >
            {!props.isTopLevel && (
              <BackButton
                aria-label="Back"
                onClick={() => {
                  try {
                    props.onHeaderClick?.();
                  } catch (error) {
                    console.error("[Page] Error in header click:", error);
                  }
                }}
                aria-hidden="true"
              ></BackButton>
            )}
            <HeaderContainer>
              <TitleContainer>
                <Title>
                  <span>
                    {props.name}
                    {typeof unwrappedCoValue.snapshot === "object" &&
                    unwrappedCoValue.snapshot &&
                    "name" in unwrappedCoValue.snapshot ? (
                      <span style={{ color: "#57534e", fontWeight: 500 }}>
                        {" "}
                        {(unwrappedCoValue.snapshot as { name: string }).name}
                      </span>
                    ) : null}
                  </span>
                </Title>
                <BadgeContainer>
                  <Badge>
                    {unwrappedCoValue.type && (
                      <TypeIcon
                        type={unwrappedCoValue.type}
                        extendedType={unwrappedCoValue.extendedType}
                      />
                    )}
                  </Badge>
                  <Badge>{props.coId}</Badge>
                </BadgeContainer>
              </TitleContainer>
            </HeaderContainer>
            <ContentContainer>
              <View
                coValue={unwrappedCoValue}
                node={props.node}
                onNavigate={props.onNavigate}
              />
              {unwrappedCoValue.extendedType !== "account" &&
                unwrappedCoValue.extendedType !== "group" &&
                unwrappedCoValue.value && (
                  <>
                    <RoleDisplay
                      node={props.node}
                      value={unwrappedCoValue.value}
                    />
                    <Text muted>
                      Owned by{" "}
                      <AccountOrGroupText
                        coId={unwrappedCoValue.value.group.id}
                        node={props.node}
                        showId
                        onClick={() => {
                          try {
                            if (unwrappedCoValue.value) {
                              props.onNavigate([
                                {
                                  coId: unwrappedCoValue.value.group.id,
                                  name: "owner",
                                },
                              ]);
                            }
                          } catch (error) {
                            console.error("[Page] Error in navigation:", error);
                          }
                        }}
                      />
                    </Text>
                  </>
                )}
            </ContentContainer>
          </PageContainer>
        );
      } catch (error) {
        console.error("[Page] Error in render:", error);
        return (
          <div style={props.style}>
            <div style={{ padding: "1rem", color: "#dc2626" }}>
              Error loading page:{" "}
              {error instanceof Error ? error.message : "Unknown error"}
            </div>
          </div>
        );
      }
    };
  },
});
