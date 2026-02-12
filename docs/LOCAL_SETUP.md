# Local Development Setup

Steps to set up local dbt development with Snowflake keypair authentication. Required for CLI usage and Claude Code sessions.

## Prerequisites

- Python 3.12 (3.14 is **not compatible** — dbt's `mashumaro` dependency crashes)
- Homebrew: `brew install python@3.12` if not already installed
- Snowflake account access with SECURITYADMIN or ACCOUNTADMIN role (for key registration)

## 1. Create virtual environment

```bash
cd ~/Development/formentera/analytics/analytics
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

This installs dbt-core, dbt-snowflake, sqlfluff, yamllint, and pre-commit.

## 2. Install dbt packages

```bash
dbt deps
```

## 3. Install pre-commit hooks

```bash
pre-commit install
```

## 4. Generate RSA keypair

```bash
mkdir -p ~/.snowflake

# Generate private key (PKCS8 format, no passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub

# Set permissions
chmod 600 ~/.snowflake/rsa_key.p8
chmod 644 ~/.snowflake/rsa_key.pub
```

## 5. Register public key in Snowflake

Extract the key body (no header/footer lines):

```bash
cat ~/.snowflake/rsa_key.pub | grep -v "^-----" | tr -d '\n' && echo
```

Run this in Snowflake as SECURITYADMIN:

```sql
ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<paste key body here>';
```

Verify the fingerprint was set:

```sql
DESC USER <YOUR_USER>;
-- Look for RSA_PUBLIC_KEY_FP — should show SHA256:...
```

Compare with your local fingerprint:

```bash
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -outform DER 2>/dev/null | openssl dgst -sha256 -binary | openssl enc -base64
```

## 6. Create dbt profiles.yml

```bash
mkdir -p ~/.dbt
```

Create `~/.dbt/profiles.yml`:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YL35090.south-central-us.azure
      user: <YOUR_SNOWFLAKE_LOGIN_NAME>
      private_key_path: <HOME_DIR>/.snowflake/rsa_key.p8
      role: DBT_ROLE
      database: FO_RAW_DB
      warehouse: DBT_WH
      schema: dbt_<your_name>
      threads: 8
```

### Connection gotchas

| Field | Correct | Wrong |
|-------|---------|-------|
| `account` | `YL35090.south-central-us.azure` | `FORMENTERA-DATAHUB` (org-account format fails for keypair JWT) |
| `user` | Your Snowflake **LOGIN_NAME** (email, e.g. `FIRST.LAST@FORMENTERAOPS.COM`) | Your Snowflake NAME (e.g. `FLAST`) |

To find your LOGIN_NAME, run in Snowflake:

```sql
DESC USER <YOUR_USER>;
-- Look for the LOGIN_NAME property
```

## 7. Verify connection

```bash
source .venv/bin/activate
dbt debug
```

All checks should pass:

```
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
  git [OK found]
  Connection test: [OK connection ok]

All checks passed!
```

## 8. Build a model to confirm end-to-end

```bash
dbt compile --select stg_oda__gl
dbt show --select stg_oda__gl --limit 5
```

## Troubleshooting

### `JWT token is invalid`

- Verify fingerprints match (step 5)
- Confirm you're using LOGIN_NAME (email), not NAME
- Confirm account is `YL35090.south-central-us.azure`, not `FORMENTERA-DATAHUB`

### `404 Not Found` on login

- Account identifier is missing the region suffix. Use `YL35090.south-central-us.azure`, not just `YL35090`

### `mashumaro.exceptions.UnserializableField`

- You're on Python 3.14+. Downgrade to Python 3.12: `python3.12 -m venv .venv`

### `Object does not exist or not authorized`

- The model hasn't been built in your dev schema yet. Run `dbt build --select <model_name>` first
