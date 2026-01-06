# External Data Migrations

## Implementation Status

**Not yet implemented** - This is a design proposal.

## Problem Statement

Schema migrations typically:
1. Change table columns (add, remove, rename, change type)
2. Add new tables
3. Transform existing data between old and new schemas

What if migrations could also:
1. **Transform data using** external URL endpoints
2. **Populate new data from** external URL endpoints

This would enable easy one-off server-side lambdas for pulling data from external systems into Jazz, making it trivial to migrate entire systems from other databases.

## Use Cases

### One-time Data Import

Migrate an existing system to Jazz by pulling all historical data:

```sql
-- Migration: 001_import_legacy_users.sql
CREATE TABLE users (
    id,
    name STRING,
    email STRING,
    created_at I64
);

IMPORT INTO users FROM 'https://api.example.com/export/users'
    WITH AUTH 'env:LEGACY_API_KEY';
```

### External Enrichment

Enrich existing data with external lookups:

```sql
-- Migration: 005_enrich_with_geo.sql
UPDATE users
SET country = EXTERNAL('https://geoip.example.com/lookup', ip_address)
WHERE country IS NULL;
```

### Periodic Sync (Future)

Define ongoing sync relationships:

```sql
-- Not a migration, but related concept
CREATE SYNC stripe_customers
    FROM 'https://api.stripe.com/v1/customers'
    WITH AUTH 'env:STRIPE_API_KEY'
    EVERY 1 HOUR;
```

## Proposed Design

### IMPORT Statement

```sql
IMPORT INTO <table> FROM '<url>'
    [WITH AUTH '<auth_spec>']
    [WITH HEADERS (<header_list>)]
    [MAPPING (<column_mapping>)];
```

**Auth Spec Options:**
- `env:VAR_NAME` - Use environment variable
- `bearer:TOKEN` - Bearer token (for testing)
- `basic:USER:PASS` - Basic auth

**Response Format:**
The URL must return JSON array of objects. Column mapping transforms field names:

```sql
IMPORT INTO users FROM 'https://api.example.com/users'
    MAPPING (
        'user_id' AS id,           -- Rename field
        'full_name' AS name,
        'email_address' AS email
    );
```

### EXTERNAL Function

For row-by-row transformations:

```sql
UPDATE documents
SET summary = EXTERNAL('https://ai.example.com/summarize', content)
WHERE summary IS NULL;
```

The function sends a POST request with the column value(s) and expects a single value response.

### Execution Context

External migrations only run on the server (not in browser/client):
- Server has network access to external APIs
- Server has access to secrets/environment variables
- Client receives the migrated data through normal sync

### Idempotency

Migrations must be idempotent. For IMPORT:
- Option 1: `ON CONFLICT SKIP` - Skip rows that already exist (by ID)
- Option 2: `ON CONFLICT UPDATE` - Update existing rows
- Option 3: `ON CONFLICT FAIL` - Fail the migration (default for safety)

```sql
IMPORT INTO users FROM 'https://api.example.com/users'
    ON CONFLICT SKIP;
```

### Error Handling

- Network failures: Retry with exponential backoff
- Partial failures: Transaction rollback (all or nothing)
- Rate limiting: Respect `Retry-After` headers
- Timeouts: Configurable per-migration

## Security Considerations

- [ ] Auth credentials must never be logged or synced
- [ ] URLs must be validated (no localhost/internal IPs by default)
- [ ] Rate limiting to prevent abuse
- [ ] Sandboxed execution for untrusted schemas

## Open Questions

- [ ] Should external migrations be a separate migration type or integrated with regular SQL?
- [ ] How to handle pagination for large datasets?
- [ ] Should we support webhooks for push-based sync?
- [ ] How does this interact with ReBAC policies? (Imported data needs ownership)
- [ ] Support for GraphQL endpoints?
- [ ] Streaming imports for very large datasets?

## Implementation Notes

External migrations would require:
1. New SQL parser extensions (IMPORT, EXTERNAL function)
2. HTTP client in the server runtime
3. Secret management for auth credentials
4. Migration runner that detects and handles external operations
