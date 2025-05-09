import {
  Body,
  Button,
  Container,
  Head,
  Heading,
  Html,
  Preview,
  Section,
  Text,
} from "@react-email/components";
import Image from "next/image";
import * as React from "react";
import {
  appName,
  button,
  code,
  codeContainer,
  container,
  h1,
  main,
  text,
} from "./Common";

export const Verify = ({
  name = undefined,
  url = undefined,
  otp = undefined,
}: {
  name?: string;
  url?: string;
  otp?: string;
}) => {
  return (
    <Html>
      <Head>
        <title>{`Verify your ${appName} account`}</title>
      </Head>
      <Preview>Verify your {appName} account</Preview>
      <Body style={main}>
        <Container style={container}>
          <Image
            src="/jazz.svg"
            alt="Jazz logo"
            width={100}
            height={100}
            priority
          />
          <Heading style={h1}>Verify your {appName} account</Heading>
          <Section>
            <Text style={text}>Hello{name ? ` ${name}` : ""},</Text>
            <Text style={text}>
              Someone recently signed up for a {appName} account using your
              email address.
              {url && !otp
                ? " If this was you, you can verify your account here:"
                : ""}
            </Text>
            {url && !otp && (
              <Button style={button} href={url}>
                Verify account
              </Button>
            )}
            {otp && !url && (
              <Section style={codeContainer}>
                <Text style={code}>{otp}</Text>
              </Section>
            )}
            <Text style={text}>
              To keep your account secure, please do not forward this email to
              anyone.
            </Text>
          </Section>
        </Container>
      </Body>
    </Html>
  );
};

export default Verify;
