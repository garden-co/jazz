import { CoID, LocalNode, RawAccount, RawCoValue } from "cojson";
import { styled } from "goober";
import { computed, defineComponent, ref, toRaw } from "vue";
import { useJazzContext } from "../../composables.js";
import { Button } from "../ui/button.js";

import { Input } from "../ui/input.js";
import { Breadcrumbs } from "./breadcrumbs.js";
import { PageStack } from "./page-stack.js";
import { usePagePath } from "./use-page-path.js";

import { Account } from "jazz-tools";
import { Heading } from "../ui/heading.js";
import { InspectorButton, type Position } from "./inspector-button.js";
import { useOpenInspector } from "./use-open-inspector.js";

// Styled components to match React version exactly
const InspectorContainer = styled("div")`
  position: fixed;
  height: 50vh;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  bottom: 0;
  left: 0;
  width: 100%;
  background-color: white;
  border-top: 1px solid var(--j-border-color);
  color: var(--j-text-color);
  z-index: 999;

  @media (prefers-color-scheme: dark) {
    background-color: var(--j-background);
  }
`;

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

const InitialForm = styled("form")`
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
  margin: 0;
`;

export const JazzInspector = defineComponent({
  name: "JazzInspector",
  props: {
    position: {
      type: String as () => Position,
      default: "right",
    },
  },
  setup(props) {
    const context = useJazzContext<Account>();

    const stableContext = computed(() => {
      try {
        const ctx = context.value;
        if (!ctx) return null;

        // Use toRaw to safely access Jazz objects
        const rawCtx = toRaw(ctx);
        const localNode = rawCtx.node;
        const me = "me" in rawCtx ? rawCtx.me : undefined;

        if (!localNode) {
          return null;
        }

        // Try to access _raw safely with toRaw
        let accountId;
        if (me) {
          try {
            const rawMe = toRaw(me);
            accountId = rawMe._raw?.id;
          } catch (error) {
            console.error("[JazzInspector] Error accessing me._raw.id:", error);
          }
        }

        return {
          hasValidContext: true,
          accountId,
          localNode,
        };
      } catch (error) {
        console.error(
          "[JazzInspector] Error in stableContext computed:",
          error,
        );
        return null;
      }
    });

    return () => {
      try {
        // Check for development environment
        if (process.env.NODE_ENV === "production") {
          return null;
        }

        const stable = stableContext.value;

        if (!stable || !stable.hasValidContext) {
          return null;
        }

        console.log("[JazzInspector] Rendering JazzInspectorInternal with:", {
          localNode: stable.localNode,
          accountId: stable.accountId,
          hasAccountId: !!stable.accountId,
        });

        return (
          <JazzInspectorInternal
            position={props.position}
            localNode={stable.localNode}
            accountId={stable.accountId}
          />
        );
      } catch (error) {
        console.error("[JazzInspector] Error in render function:", error);
        return null;
      }
    };
  },
});

export const JazzInspectorInternal = defineComponent({
  name: "JazzInspectorInternal",
  props: {
    position: {
      type: String as () => Position,
      default: "right",
    },
    localNode: Object as () => LocalNode,
    accountId: String as any,
  },
  setup(props) {
    const [open, setOpen, restoreFromStorage] = useOpenInspector();
    const coValueId = ref<CoID<RawCoValue> | "">("");
    const { path, addPages, goToIndex, goBack, setPage } = usePagePath();
    const hasStableContext = computed(
      () => !!(props.localNode && props.accountId),
    );

    const handleCoValueIdSubmit = (e: any) => {
      e.preventDefault();
      if (coValueId.value) {
        setPage(coValueId.value);
      }
      coValueId.value = "";
    };

    const handleOpenInspector = () => {
      if (!hasStableContext.value) {
        return;
      }
      setOpen(true);
    };

    return () => {
      if (!open.value) {
        return (
          <InspectorButton
            position={props.position}
            onClick={handleOpenInspector}
          />
        );
      }

      return (
        <InspectorContainer>
          <HeaderContainer>
            <Breadcrumbs path={path.value} onBreadcrumbClick={goToIndex} />
            <Form onSubmit={handleCoValueIdSubmit}>
              {path.value.length !== 0 && (
                <Input
                  label="CoValue ID"
                  style={{ fontFamily: "monospace" }}
                  hideLabel
                  placeholder="co_z1234567890abcdef123456789"
                  modelValue={coValueId.value}
                  onUpdate:modelValue={(value: string) =>
                    (coValueId.value = value as CoID<RawCoValue>)
                  }
                />
              )}
            </Form>
            <Button
              variant="plain"
              type="button"
              onClick={() => setOpen(false)}
            >
              Close
            </Button>
          </HeaderContainer>

          <PageStack
            path={path.value}
            node={props.localNode}
            goBack={goBack}
            addPages={addPages}
          >
            {path.value.length <= 0 && (
              <InitialForm
                onSubmit={handleCoValueIdSubmit}
                aria-hidden={path.value.length !== 0}
              >
                <Heading>Jazz CoValue Inspector</Heading>
                <Input
                  label="CoValue ID"
                  style={{ minWidth: "21rem", fontFamily: "monospace" }}
                  hideLabel
                  placeholder="co_z1234567890abcdef123456789"
                  modelValue={coValueId.value}
                  onUpdate:modelValue={(value: string) =>
                    (coValueId.value = value as CoID<RawCoValue>)
                  }
                />
                <Button type="submit" variant="primary">
                  Inspect CoValue
                </Button>

                {(() => {
                  console.log("[JazzInspectorInternal] accountId check:", {
                    accountId: props.accountId,
                    hasAccountId: !!props.accountId,
                    accountIdType: typeof props.accountId,
                  });
                  return (
                    props.accountId && (
                      <>
                        <OrText>or</OrText>

                        <Button
                          variant="secondary"
                          onClick={() => {
                            coValueId.value = props.accountId!;
                            setPage(props.accountId!);
                          }}
                        >
                          Inspect my account
                        </Button>
                      </>
                    )
                  );
                })()}
              </InitialForm>
            )}
          </PageStack>
        </InspectorContainer>
      );
    };
  },
});
