import {
  Body,
  Button,
  Container,
  Head,
  Heading,
  Html,
  Preview,
  Row,
  Section,
  Text,
} from "@react-email/components";
import Image from "next/image";
import * as React from "react";
import { appName, button, container, h1, main, text } from "./Common";

export const Welcome = () => {
  return (
    <Html>
      <Head>
        <title>{`Welcome to ${appName}`}</title>
      </Head>
      <Preview>Welcome to {appName}</Preview>
      <Body style={main}>
        <Container style={container}>
          <Image
            src="/jazz.svg"
            alt="Jazz logo"
            width={100}
            height={100}
            priority
          />
          <Heading style={h1}>Welcome to {appName}</Heading>
          <Section>
            <Row>
              <Text style={text}>
                Congratulations! You have successfully registered for {appName}.
                We are excited to have you on board!
              </Text>
            </Row>
          </Section>
          <Section>
            <Button style={button} href="/">
              Go home
            </Button>
          </Section>
        </Container>
      </Body>
    </Html>
  );
};

export default Welcome;
