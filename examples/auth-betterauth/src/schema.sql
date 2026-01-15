-- Jazz Schema with BetterAuth-aware policies
--
-- This schema demonstrates how to use JWT claims from BetterAuth
-- in Jazz policies for fine-grained access control.

-- Users table (auto-provisioned from BetterAuth)
CREATE TABLE users (
  name STRING,
  email STRING,
  external_id STRING NOT NULL,
  subscription_tier STRING
);

-- Personal documents - only owner can see
CREATE TABLE documents (
  title STRING NOT NULL,
  content STRING,
  author REFERENCES users NOT NULL
);

CREATE POLICY ON documents FOR SELECT
  WHERE author = @viewer;

CREATE POLICY ON documents FOR INSERT
  CHECK (@new.author = @viewer);

CREATE POLICY ON documents FOR UPDATE
  WHERE author = @viewer;

CREATE POLICY ON documents FOR DELETE
  WHERE author = @viewer;

-- Organization documents - scoped by orgId claim
CREATE TABLE org_documents (
  title STRING NOT NULL,
  content STRING,
  org_id STRING NOT NULL,
  author REFERENCES users NOT NULL
);

-- Only users in the same organization can see documents
CREATE POLICY ON org_documents FOR SELECT
  WHERE org_id = @viewer.claims.orgId;

-- Only users in the org can create documents
CREATE POLICY ON org_documents FOR INSERT
  CHECK (
    @new.org_id = @viewer.claims.orgId
    AND @new.author = @viewer
  );

-- Authors can update their own documents within their org
CREATE POLICY ON org_documents FOR UPDATE
  WHERE org_id = @viewer.claims.orgId
    AND author = @viewer;

-- Premium features - subscription tier check
CREATE TABLE premium_features (
  name STRING NOT NULL,
  description STRING,
  min_tier STRING NOT NULL
);

-- Only pro/enterprise users can see premium features
CREATE POLICY ON premium_features FOR SELECT
  WHERE @viewer.claims.subscriptionTier = 'pro'
     OR @viewer.claims.subscriptionTier = 'enterprise';

-- Admin settings - role-based access
CREATE TABLE admin_settings (
  key STRING NOT NULL,
  value STRING
);

-- Only admins can read/update settings
CREATE POLICY ON admin_settings FOR SELECT
  WHERE @viewer.claims.roles CONTAINS 'admin';

CREATE POLICY ON admin_settings FOR UPDATE
  WHERE @viewer.claims.roles CONTAINS 'admin';

-- Shared documents with team membership
CREATE TABLE team_documents (
  title STRING NOT NULL,
  content STRING,
  team_id STRING NOT NULL,
  author REFERENCES users NOT NULL
);

-- Users can see documents for teams they belong to
CREATE POLICY ON team_documents FOR SELECT
  WHERE team_id IN @viewer.claims.teams;

CREATE POLICY ON team_documents FOR INSERT
  CHECK (
    @new.team_id IN @viewer.claims.teams
    AND @new.author = @viewer
  );
