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

export const Login = ({
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
        <title>
          {url && !otp
            ? `Your login link for ${appName}`
            : `Your login code for ${appName}`}
        </title>
      </Head>
      <Preview>
        {url && !otp
          ? `Your login link for ${appName}`
          : `Your login code for ${appName}`}
      </Preview>
      <Body style={main}>
        <Container style={container}>
          <Image
            src="/jazz.svg"
            alt="Jazz logo"
            width={100}
            height={100}
            priority
          />
          <Heading style={h1}>
            {url && !otp
              ? `Your login link for ${appName}`
              : `Your login code for ${appName}`}
          </Heading>
          <Section>
            <Text style={text}>Hello{name ? ` ${name}` : ""},</Text>
            <Text style={text}>
              Someone recently attempted to login to {appName} using your email
              address.
              {url && !otp ? " If this was you, you can login here:" : ""}
            </Text>
            {url && !otp && (
              <Button style={button} href={url}>
                Login
              </Button>
            )}
            {otp && !url && (
              <Section style={codeContainer}>
                <Text style={code}>{otp}</Text>
              </Section>
            )}
            <Text style={text}>
              To keep your account secure, please do not forward this email to
              anyone. If you did not try to login, just ignore and delete this
              message.
            </Text>
          </Section>
        </Container>
      </Body>
    </Html>
  );
};

export default Login;
