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

export const Reset = ({
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
        <title>{`Reset your ${appName} password`}</title>
      </Head>
      <Preview>Reset your {appName} password</Preview>
      <Body style={main}>
        <Container style={container}>
          <Image
            src="/jazz.svg"
            alt="Jazz logo"
            width={100}
            height={100}
            priority
          />
          <Heading style={h1}>Reset your {appName} password</Heading>
          <Section>
            <Text style={text}>Hello{name ? ` ${name}` : ""},</Text>
            <Text style={text}>
              Someone recently requested a password change for your {appName}{" "}
              account.
              {url && !otp
                ? " If this was you, you can set a new password here:"
                : ""}
            </Text>
            {url && !otp && (
              <Button style={button} href={url}>
                Reset password
              </Button>
            )}
            {otp && !url && (
              <Section style={codeContainer}>
                <Text style={code}>{otp}</Text>
              </Section>
            )}
            <Text style={text}>
              If you do not want to change your password or did not request
              this, just ignore and delete this message.
            </Text>
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

export default Reset;
