# Target Governance Policy

This document defines the governance standard for the Urban Analytics platform and, more
importantly, **separates what is actually enforced in this educational project from what is a
target production standard that the Fabric Trial environment does not automate**.

Back to the [project overview](../README.md) · [data dictionary](data_dictionary.md) ·
[architecture decisions](architecture_decisions.md)

---

## 1. Status at a glance

| Control | Status in this project | Where it lives |
|---------|------------------------|----------------|
| Workspace RBAC (Admin / Member / Contributor / Viewer) | **Active** — manual role assignment in Fabric | Fabric workspace |
| No PII ingested | **Active** — enforced by source selection and schema review | Bronze/Silver/Gold |
| Audit log `data_quality_logs` | **Active** — append-only, written by every run | `Lakehouse_Silver` |
| No secrets in the repository | **Active** — placeholder only, `.gitignore` | Git repository |
| Service Principal authentication | **Target standard** — not configured | Fabric connections |
| Automatic partition archival | **Target standard** — retention runbook is manual | OneLake / storage |
| Key rotation schedule | **Target standard** — manual rotation | Key Vault / variable library |
| Pre-commit hooks and secret scanning | **Target standard** — not installed | Developer workstation / CI |

The rest of this document explains each row.

---

## 2. What is active in this project

### 2.1 Access control — Fabric workspace RBAC

Access is granted through **workspace roles**, not per-table grants, so one role change covers
notebooks, pipelines, lakehouses, the warehouse and the semantic model.

| Role | Intended holder | Effective permissions |
|------|-----------------|-----------------------|
| **Admin** | Platform owner (1–2 people) | Full control: members, connections, schedules, capacity settings; can delete items and rotate credentials. |
| **Member** | Data engineers | Create/edit notebooks, pipelines and SQL objects; run `pl_master_e2e`; read/write all lakehouses and the warehouse. No membership management. |
| **Contributor** | Senior analysts / reviewers | Publish and edit content and run pipelines; no member management. Intended for temporary collaboration. |
| **Viewer** | Business users | Read-only: view the report and semantic model, query the warehouse with `ReadData` only. No writes, no pipeline execution. |

Rules that keep this model auditable:

* Row-level and object-level security, if ever needed, belong in the semantic model
  (`UrbanAnalytics_SemanticModel`), not duplicated per table.
* Service principals used for scheduled runs are scoped to this workspace only — no tenant-wide
  permissions (see §3.1 for the target state).
* People join and leave a project by role assignment; individual table grants are avoided.

### 2.2 No personal data (PII)

| Dataset | Classification | PII | Rationale |
|---------|----------------|-----|-----------|
| NYC TLC Yellow Taxi | **Public** source / **Internal** curated | **None** | The TLC release is anonymised: no passenger or driver identifiers, no names, no card numbers, no addresses — only timestamps, zone IDs, payment-type codes and amounts. |
| OpenAQ v3 air quality | **Public** source / **Internal** curated | **None** | Station metadata (name, coordinates, provider) and environmental measurements only. |
| ECB USD/EUR rates | **Public** | **None** | Published reference rates. |
| World Bank US GDP | **Public** | **None** | Published macro-economic indicator. |
| `data_quality_logs` | **Internal** | **None** | Table names, row counts and rule outcomes; `details_json` holds rule metadata, never row-level values. |
| Power BI report / semantic model | **Internal** | **None** | Aggregated business metrics. |

Consequences:

* **No PII is ingested, so no GDPR subject-rights workflows (export/erasure) are required.** This is
  a deliberate design constraint: a future source that introduces personal data must be
  pseudonymised in Bronze and this table must be updated *before* promotion to Silver.
* **Public sources do not make public outputs.** The curated warehouse, audit logs and report are
  **Internal** — visible inside the Fabric tenant only.
* Coordinates are station-level (public infrastructure), not device-level, so they do not enable
  individual tracking.

### 2.3 Audit log retention — `data_quality_logs`

* The table is **append-only**: nothing in the pipeline updates or deletes historical rows.
* Every run inserts **one row per validated table** (five rows per full refresh), i.e. ≈ 1 800 rows
  per year — small enough that retention is cheap.
* `details_json` is truncated to 4 000 characters, which bounds row size while keeping the failing
  rule visible.
* **Current practice:** all history is kept in the lakehouse; the table is never used as a Gold
  source, so pruning it cannot affect the warehouse or the report.
* **Target practice:** 12 months hot + archive (see §3.2).

### 2.4 Secrets management

* **No secret is stored in this repository.** `OPENAQ_API_KEY` is the literal placeholder
  `"YOUR_API_KEY"`; the other sources need no credentials.
* At runtime the key is injected from outside the code: a Fabric **variable library**, a
  **Key Vault-backed connection**, or an environment variable read inside the notebook. It is never
  printed and never written into cell outputs.
* `.gitignore` excludes `.env` / `.env.*`, `.ipynb_checkpoints/`, `.jupyter/` and `__pycache__/`.
* Workspace connections (HTTP, Lakehouse) keep their credentials in Fabric, not in the pipeline JSON.

---

## 3. Target production standard (not automated in the Trial environment)

The controls below are the standard this project is designed for. They are **not implemented
automatically here**, because the Fabric Trial capacity does not provide the supporting services
(Key Vault integration, automated lifecycle jobs, CI runners) and because they add operational
overhead that is out of scope for an educational build.

### 3.1 Service Principal authentication

**Target:** all automated runs authenticate with a **Service Principal** (or managed identity)
instead of a personal account.

* The principal gets **Member** on this workspace only, with no tenant-wide permissions.
* Its client secret lives in **Azure Key Vault** and is referenced by the Fabric connection; nothing
  is stored in the repository.
* Pipelines and the daily schedule run under the principal, so a person leaving the project does not
  break the nightly load.
* **Gap today:** connections use the developer's credentials; the schedule would stop if that
  account lost access.

### 3.2 Automatic partition archival

**Target:** a scheduled maintenance job that exports partitions older than the hot window to
long-term storage and removes them from the live table.

* Hot window: **12 months** in `data_quality_logs`; archive as Parquet/Delta with the same schema so
  history stays queryable.
* Archival is **idempotent** and logged (job name, row count, date range).
* **Gap today:** retention is a documented runbook executed manually; no lifecycle job exists.

### 3.3 Key rotation schedule

**Target:** calendar-driven rotation of the OpenAQ key and any future credentials.

* Rotation every **90 days**, and immediately on suspected exposure.
* Because the notebook only *reads* the key, rotation requires no code change — update the variable
  library / Key Vault entry and re-run.
* Old keys are revoked after a successful validation run.
* **Gap today:** rotation is manual and ad hoc.

### 3.4 Pre-commit hooks and secret scanning

**Target:** client-side and CI checks that block a commit containing a credential.

* **pre-commit** hook running a secret scanner (e.g. `gitleaks` / `detect-secrets`) plus a check for
  large binary files and notebook outputs.
* The same scan runs in CI on every push, so a `--no-verify` bypass is still caught.
* **Gap today:** verification is manual (a text scan before each commit); no hook is installed and
  there is no CI pipeline.

---

## 4. Ownership and review

| Area | Owner | Review cadence |
|------|-------|----------------|
| Workspace roles and membership | Platform owner (Admin) | On change, and quarterly |
| Data classification / PII assessment | Data engineer | On every new source |
| Audit-log retention | Platform owner | Quarterly |
| Secret rotation | Platform owner | 90 days |
| This policy | Platform owner | Every major release |

Any new data source must be added to the classification table in §2.2 **before** it is promoted to
Silver; see the extensibility guide in
[architecture_decisions.md](architecture_decisions.md#extensibility--how-to-add-a-new-data-source).
