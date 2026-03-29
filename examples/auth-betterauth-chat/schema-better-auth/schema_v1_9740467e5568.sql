CREATE TABLE better_auth_account (
    accountId TEXT NOT NULL,
    providerId TEXT NOT NULL,
    userId UUID REFERENCES better_auth_user NOT NULL,
    accessToken TEXT,
    refreshToken TEXT,
    idToken TEXT,
    accessTokenExpiresAt TIMESTAMP,
    refreshTokenExpiresAt TIMESTAMP,
    scope TEXT,
    password TEXT,
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL
);

CREATE TABLE better_auth_jwks (
    publicKey TEXT NOT NULL,
    privateKey TEXT NOT NULL,
    createdAt TIMESTAMP NOT NULL,
    expiresAt TIMESTAMP
);

CREATE TABLE better_auth_session (
    expiresAt TIMESTAMP NOT NULL,
    token TEXT NOT NULL,
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL,
    ipAddress TEXT,
    userAgent TEXT,
    userId UUID REFERENCES better_auth_user NOT NULL,
    impersonatedBy TEXT
);

CREATE TABLE better_auth_user (
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    emailVerified BOOLEAN NOT NULL,
    image TEXT,
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL,
    role TEXT,
    banned BOOLEAN,
    banReason TEXT,
    banExpires TIMESTAMP
);

CREATE TABLE better_auth_verification (
    identifier TEXT NOT NULL,
    value TEXT NOT NULL,
    expiresAt TIMESTAMP NOT NULL,
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL
);