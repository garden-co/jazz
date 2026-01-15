-- Jazz Schema with WorkOS-aware policies
--
-- This schema demonstrates permission-based access control using
-- automatic claims from WorkOS (org_id, role, permissions array).

-- Users table (auto-provisioned from WorkOS)
CREATE TABLE users (
  name STRING,
  email STRING,
  external_id STRING NOT NULL,
  org_id STRING NOT NULL
);

-- Documents with sensitivity levels
CREATE TABLE documents (
  title STRING NOT NULL,
  content STRING,
  org_id STRING NOT NULL,
  sensitivity STRING NOT NULL,
  author REFERENCES users NOT NULL
);

-- Read: Public docs for anyone in org, others need permission
CREATE POLICY ON documents FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND (
      sensitivity = 'public'
      OR @viewer.claims.permissions CONTAINS 'documents:read'
    );

-- Create: Need write permission
CREATE POLICY ON documents FOR INSERT
  CHECK (
    @new.org_id = @viewer.claims.org_id
    AND @new.author = @viewer
    AND @viewer.claims.permissions CONTAINS 'documents:write'
  );

-- Update: Need write permission, only own documents
CREATE POLICY ON documents FOR UPDATE
  WHERE org_id = @viewer.claims.org_id
    AND author = @viewer
    AND @viewer.claims.permissions CONTAINS 'documents:write';

-- Delete: Need delete permission
CREATE POLICY ON documents FOR DELETE
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'documents:delete';

-- Admin settings - role-based
CREATE TABLE settings (
  key STRING NOT NULL,
  value STRING,
  org_id STRING NOT NULL
);

-- Only admins can manage settings
CREATE POLICY ON settings FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.role = 'admin';

CREATE POLICY ON settings FOR UPDATE
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.role = 'admin';

-- User management - permission-based
CREATE POLICY ON users FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'users:read';

CREATE POLICY ON users FOR INSERT
  CHECK (
    @new.org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'users:invite'
  );

-- Audit logs - read-only for users with audit permission
CREATE TABLE audit_logs (
  action STRING NOT NULL,
  actor_id STRING NOT NULL,
  resource_type STRING NOT NULL,
  resource_id STRING NOT NULL,
  org_id STRING NOT NULL,
  timestamp I64 NOT NULL
);

CREATE POLICY ON audit_logs FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'audit:read';

-- Projects with team-based access
CREATE TABLE projects (
  name STRING NOT NULL,
  description STRING,
  org_id STRING NOT NULL,
  team_id STRING NOT NULL
);

-- Access based on team membership (teams claim from directory sync)
CREATE POLICY ON projects FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND team_id IN @viewer.claims.groups;

CREATE POLICY ON projects FOR UPDATE
  WHERE org_id = @viewer.claims.org_id
    AND team_id IN @viewer.claims.groups
    AND @viewer.claims.permissions CONTAINS 'projects:edit';
