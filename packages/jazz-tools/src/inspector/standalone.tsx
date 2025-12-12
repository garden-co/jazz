import React from "react";
import { HashRouterProvider } from "./router";
import { setup, styled } from "goober";
import { NodeContext, NodeProvider } from "./contexts/node";
import { Header } from "./viewer/header";
import { GlobalStyles } from "./ui/global-styles";
import { PageStack } from "./viewer/page-stack";
import { AccountSwitcher } from "./account-switcher";

type InspectorAppProps = {
  defaultSyncServer?: string;
};

setup(React.createElement);

export default function InspectorStandalone(props: InspectorAppProps) {
  return (
    <HashRouterProvider>
      <NodeProvider>
        <InspectorContainer as={GlobalStyles}>
          <Header>
            <AccountSwitcher defaultSyncServer={props.defaultSyncServer} />
          </Header>
          <NodeContext.Consumer>
            {({ accountID }) =>
              accountID ? (
                <PageStack />
              ) : (
                <CenteredMessage>
                  Select an account to connect to the inspector.
                </CenteredMessage>
              )
            }
          </NodeContext.Consumer>
        </InspectorContainer>
      </NodeProvider>
    </HashRouterProvider>
  );
}

const InspectorContainer = styled("div")`
  height: 100vh;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  color: #44403c; /* text-stone-700 */
  background-color: #fff; /* bg-white */

  @media (prefers-color-scheme: dark) {
    color: #d6d3d1; /* text-stone-300 */
    background-color: #0c0a09; /* bg-stone-950 */
  }
`;

const CenteredMessage = styled("p")`
  text-align: center;
  margin: 0;
  padding: 1rem;
  color: var(--j-text-color);
`;
