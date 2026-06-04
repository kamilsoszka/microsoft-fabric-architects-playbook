# microsoft-fabric-architects-playbook

> A comprehensive **Microsoft Fabric architecture and DevOps playbook** for designing scalable analytics platforms with OneLake, medallion architecture, lakehouse/warehouse patterns, Direct Lake semantic models, real-time analytics, security, governance, and lifecycle management.

---

## Table of Contents

1. [Repository Goals](#repository-goals)
2. [Recommended Repository Name](#recommended-repository-name)
3. [Architecture Principles](#architecture-principles)
4. [Platform Foundations](#platform-foundations)
5. [Data Engineering and Ingestion](#data-engineering-and-ingestion)
6. [Lakehouse, Warehouse, and Semantic Layer Design](#lakehouse-warehouse-and-semantic-layer-design)
7. [Real-Time Intelligence](#real-time-intelligence)
8. [Security and Governance](#security-and-governance)
9. [Performance and Cost Optimization](#performance-and-cost-optimization)
10. [DevOps, Git Integration, and Deployment Pipelines](#devops-git-integration-and-deployment-pipelines)
11. [Advanced Techniques](#advanced-techniques)
12. [Definition of Done Checklist](#definition-of-done-checklist)
13. [How to Create the GitHub Repository and Upload This README](#how-to-create-the-github-repository-and-upload-this-readme)
14. [References](#references)

---

## Repository Goals

This repository documents practical, field-tested guidance for building maintainable, governed, and high-performance Microsoft Fabric solutions across:

- OneLake architecture
- Medallion data design (Bronze / Silver / Gold)
- Lakehouse and Warehouse implementation
- Data Factory pipelines and Dataflows Gen2
- Spark notebooks and SQL-based transformations
- Direct Lake semantic models and Power BI consumption
- Real-Time Intelligence with Eventstreams, Eventhouse, and KQL
- Security, permissions, and governance
- Git integration, deployment pipelines, and ALM automation

---

## Recommended Repository Name

The recommended GitHub repository name for this documentation is:

```text
microsoft-fabric-architects-playbook
```

### Why this name works well

- It clearly identifies the repository as a Microsoft Fabric architecture guide.
- It matches enterprise GitHub naming conventions.
- It aligns nicely with a companion repository such as `powerbi-architects-playbook`.

Alternative names:

```text
fabric-architects-playbook
fabric-platform-playbook
fabric-devops-playbook
fabric-data-platform-playbook
```

---

## Architecture Principles

| Principle | Description | Why it matters |
|---|---|---|
| One copy of data | Reuse OneLake instead of duplicating data across tools | Reduces storage sprawl and governance overhead |
| Layer by quality | Organize data as Bronze, Silver, Gold | Improves trust, lineage, and maintainability |
| Separate platform from consumption | Build reusable data products before reports | Reduces report-specific rework |
| Optimize for both performance and operability | Design for query speed and maintainable refresh/processing | Better user experience and lower support cost |
| Govern by design | Apply permissions, naming, ownership, and lifecycle controls early | Reduces security and compliance risk |
| Treat metadata as code | Version Fabric assets where supported | Safer collaboration and repeatable delivery |

---

## Platform Foundations

### 1) OneLake Strategy

#### Why this matters

**Business rationale**
- OneLake reduces fragmentation by providing a single logical data lake for the organization.
- Shared storage and reuse reduce duplicated engineering effort and lower total cost of ownership.

**Technical rationale**
- Fabric stores analytics items in OneLake and supports shared access patterns across lakehouse, warehouse, notebooks, and semantic models.
- Open formats and shared storage simplify interoperability.

#### How to do it step by step

1. Define data domains or product areas.
2. Create workspaces aligned to ownership and environment strategy.
3. Store raw, refined, and curated data in OneLake-oriented structures.
4. Avoid creating duplicate copies unless there is a justified isolation, residency, or performance need.
5. Define naming standards for workspaces, lakehouses, warehouses, shortcuts, and semantic models.

**Example naming convention**

```text
<domain>-<layer>-<environment>
finance-bronze-dev
finance-silver-test
finance-gold-prod
```

#### Effect / gain

- Better data discoverability
- Lower duplication of files and transformations
- Easier governance and access management

#### What to avoid

- Creating separate storage silos for every team without a domain strategy
- Replicating the same datasets across many workspaces “just in case”
- Mixing raw ingestion and curated consumption assets without boundaries

---

### 2) Medallion Architecture

#### Why this matters

**Business rationale**
- Bronze, Silver, and Gold separation makes ownership, quality, and data contracts clearer.
- It helps teams trace KPI issues back to specific transformation layers.

**Technical rationale**
- Fabric recommends medallion architecture in OneLake-based implementations.
- Layering improves testability, reusability, and downstream semantic model stability.

#### How to do it step by step

1. **Bronze**: ingest raw data unchanged where possible.
2. **Silver**: standardize types, deduplicate, validate, and conform keys.
3. **Gold**: build analytics-ready tables for reporting and semantic models.
4. Keep transformation responsibilities explicit between layers.
5. Document source-to-target lineage.

**Example PySpark Bronze → Silver transformation**

```python
from pyspark.sql import functions as F

bronze_df = spark.read.format("delta").load("Tables/bronze_sales")

silver_df = (
    bronze_df
    .filter(F.col("OrderDate").isNotNull())
    .dropDuplicates(["SalesOrderNumber", "SalesOrderLineNumber"])
    .withColumn("SalesAmount", F.col("SalesAmount").cast("decimal(18,2)"))
    .withColumn("OrderDate", F.to_date("OrderDate"))
)

silver_df.write.mode("overwrite").format("delta").save("Tables/silver_sales")
```

**Example SQL Silver → Gold aggregation**

```sql
CREATE OR REPLACE TABLE gold_sales_daily AS
SELECT
    OrderDate,
    CustomerKey,
    ProductKey,
    SUM(SalesAmount) AS SalesAmount,
    SUM(OrderQuantity) AS OrderQuantity
FROM silver_sales
GROUP BY
    OrderDate,
    CustomerKey,
    ProductKey;
```

#### Effect / gain

- Better data quality control
- Lower rework in semantic models and reports
- Easier incident analysis when metrics are wrong

#### What to avoid

- Putting business logic directly in Bronze
- Building reports directly on raw ingestion tables
- Mixing cleansing, conformance, and consumption logic in one artifact

---

### 3) Workspace and Environment Design

#### Why this matters

**Business rationale**
- Clear ownership boundaries reduce confusion over who supports which assets.

**Technical rationale**
- Fabric organizes collaboration, item permissions, and lifecycle management around workspaces.

#### How to do it step by step

1. Decide whether workspaces are organized by domain, environment, or product.
2. Separate **Dev / Test / Prod** workspaces.
3. Assign workspace roles by least privilege.
4. Use deployment pipelines or ALM automation to move content between environments.
5. Define ownership and support contacts per workspace.

#### Effect / gain

- Cleaner promotions and rollback paths
- Lower risk of accidental production edits
- Stronger auditability of changes

#### What to avoid

- One giant workspace for every asset in the company
- Developers working directly in production workspaces
- No owner assigned to critical data products

---

## Data Engineering and Ingestion

### 1) Pipelines and Dataflows Gen2

#### Why this matters

**Business rationale**
- Reliable ingestion is the foundation for trustworthy analytics.

**Technical rationale**
- Fabric Data Factory capabilities orchestrate movement and transformation across sources and Fabric items.

#### How to do it step by step

1. Use **Pipelines** to orchestrate ingestion, notebook execution, SQL scripts, and dependencies.
2. Use **Dataflows Gen2** for reusable low-code transformations when they fit the scenario.
3. Parameterize source locations and environment-specific settings.
4. Add retries, alerts, and dependency control.
5. Land data in Bronze with consistent naming and metadata.

**Example pseudo pipeline sequence**

```text
1. Copy ERP sales data to bronze layer
2. Execute notebook for cleansing and conformance
3. Run SQL script for gold tables
4. Refresh semantic model or trigger downstream task
5. Log status and notify on failure
```

#### Effect / gain

- More repeatable ingestion operations
- Better orchestration visibility
- Lower manual intervention in refresh windows

#### What to avoid

- Hard-coded parameters across environments
- Monolithic pipelines with no modularity
- Silent failures without alerting or logging

---

### 2) Notebooks and Spark Transformations

#### Why this matters

**Business rationale**
- Notebooks enable scalable engineering patterns for cleansing, enrichment, and large transformations.

**Technical rationale**
- Spark is appropriate when SQL-only transformations become too complex, too large, or too iterative.

#### How to do it step by step

1. Use notebooks for transformations requiring Spark-scale processing.
2. Standardize notebook structure:
   - parameters
   - imports
   - read
   - validate
   - transform
   - write
   - log
3. Write outputs as Delta tables.
4. Keep notebook logic deterministic and idempotent.
5. Externalize reusable logic to shared utilities when possible.

**Example parameterized notebook snippet**

```python
layer = "silver"
source_table = "bronze_sales"
target_table = f"{layer}_sales"

source_df = spark.read.table(source_table)

result_df = source_df.filter("OrderStatus <> 'Cancelled'")
result_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
```

#### Effect / gain

- Better scalability for heavy transformations
- Reusable engineering patterns across projects
- Easier automation when notebooks are standardized

#### What to avoid

- Manual notebook edits in production with no version control
- Business-critical logic embedded in ad hoc analyst notebooks
- Writing outputs with inconsistent schemas between runs

---

### 3) Shortcuts and Data Reuse

#### Why this matters

**Business rationale**
- Reusing data without physically copying it can reduce both storage cost and duplication.

**Technical rationale**
- Shortcuts help create logical access patterns to data already present in OneLake or external storage systems.

#### How to do it step by step

1. Identify datasets that should be reused rather than copied.
2. Create shortcuts into approved source data locations.
3. Validate permissions, lineage, and refresh expectations.
4. Document the dependency so downstream teams understand ownership.

#### Effect / gain

- Lower storage duplication
- Faster onboarding of consuming teams
- Better alignment with data product thinking

#### What to avoid

- Using shortcuts without ownership agreements
- Shortcut chains that become difficult to understand or troubleshoot
- Mixing reusable source data with team-local scratch assets

---

### 4) Data Quality and Validation

#### Why this matters

**Business rationale**
- Trusted analytics require explicit quality gates.

**Technical rationale**
- Data contracts, validation checks, and exception logging keep bad data from contaminating Gold assets.

#### How to do it step by step

1. Define critical quality checks:
   - null checks
   - uniqueness checks
   - referential integrity checks
   - freshness checks
   - distribution checks
2. Run validation in Silver before publishing Gold.
3. Store rejected rows or validation results separately.
4. Add monitoring and alerting.

**Example PySpark validation**

```python
from pyspark.sql import functions as F

invalid_orders = silver_df.filter(F.col("CustomerKey").isNull() | F.col("OrderDate").isNull())
valid_orders = silver_df.subtract(invalid_orders)

invalid_orders.write.mode("overwrite").format("delta").save("Tables/quality_invalid_orders")
valid_orders.write.mode("overwrite").format("delta").save("Tables/gold_valid_orders")
```

#### Effect / gain

- Lower KPI defect rate
- Better observability of source-system issues
- Faster root-cause analysis during incidents

#### What to avoid

- Replacing all faulty values with defaults such as zero
- Publishing unvalidated Silver tables as Gold
- Hiding data quality failures instead of surfacing them

---

## Lakehouse, Warehouse, and Semantic Layer Design

### 1) Lakehouse vs Warehouse Decision

#### Why this matters

**Business rationale**
- Different consumers and workloads need different serving patterns.

**Technical rationale**
- Fabric supports both lakehouse and warehouse architectures, and they can coexist over shared storage foundations.

#### How to do it step by step

1. Use **Lakehouse** when Spark, notebooks, open-format engineering, or data science are primary requirements.
2. Use **Warehouse** when SQL-centric analytics, relational serving, and warehouse-style development are primary requirements.
3. Combine them when engineering and SQL consumption both matter.
4. Publish only stable, trusted Gold assets to downstream semantic models.

#### Effect / gain

- Better fit between user personas and serving layer
- Lower friction between data engineering and analytics teams

#### What to avoid

- Treating every dataset as a warehouse by default
- Treating every analytics requirement as a notebook problem
- Building semantic models directly on unstable Silver assets

---

### 2) Direct Lake Semantic Models

#### Why this matters

**Business rationale**
- Direct Lake can deliver fast interactive analytics over large Fabric-managed data without full import refresh overhead.

**Technical rationale**
- Direct Lake uses VertiPaq for query speed while sourcing from Delta tables in OneLake.
- It is especially suitable for large Gold-layer analytics scenarios in Fabric.

#### How to do it step by step

1. Design Gold tables with analytic consumption in mind.
2. Create a semantic model from a Fabric item or in Power BI Desktop.
3. Choose the appropriate Direct Lake mode based on your scenario.
4. Keep the model simple and business-friendly.
5. Validate relationships, measure patterns, and security behavior.
6. Use explicit measures instead of relying only on implicit aggregations.

**Example DAX measures**

```DAX
[Sales Amount] = SUM ( 'Fact Sales'[SalesAmount] )

[Gross Margin] =
SUM ( 'Fact Sales'[SalesAmount] ) - SUM ( 'Fact Sales'[CostAmount] )

[Gross Margin %] =
DIVIDE ( [Gross Margin], [Sales Amount] )
```

#### Effect / gain

- Lower semantic model refresh overhead compared with full import scenarios
- Faster time-to-consumption for large Gold assets
- Better alignment between Fabric engineering and Power BI consumption

#### What to avoid

- Publishing Direct Lake models on poorly curated Gold tables
- Overcomplicating the semantic model when the serving layer can do more work upstream
- Ignoring storage mode and security implications

---

### 3) Dimensional Modeling for Fabric Semantic Layers

#### Why this matters

**Business rationale**
- BI consumers still need stable facts, dimensions, and clear KPIs—even in a lake-first platform.

**Technical rationale**
- Star schema remains the preferred semantic modeling pattern for accurate and performant analytic models.

#### How to do it step by step

1. Build Gold fact and dimension tables.
2. Keep facts narrow and numeric-heavy.
3. Use a proper date dimension.
4. Resolve many-to-many with bridge structures where appropriate.
5. Create explicit measures.
6. Hide technical columns.

**Example Date dimension in SQL**

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    d AS CalendarDate,
    YEAR(d) AS CalendarYear,
    MONTH(d) AS MonthNumber,
    DATE_FORMAT(d, 'MMMM') AS MonthName,
    CONCAT(YEAR(d), '-', LPAD(MONTH(d), 2, '0')) AS YearMonth
FROM (
    SELECT explode(sequence(to_date('2024-01-01'), to_date('2030-12-31'), interval 1 day)) AS d
) x;
```

#### Effect / gain

- Simpler report development
- More reliable DAX
- Lower risk of ambiguous or incorrect totals

#### What to avoid

- Exposing raw normalized operational schemas directly to analysts
- Using many-to-many relationships as a shortcut for poor design
- Embedding all KPI logic in report visuals

---

### 4) SQL Endpoint and SQL Development Practices

#### Why this matters

**Business rationale**
- SQL remains the common language for analytics teams and operational support.

**Technical rationale**
- SQL endpoints and warehouse SQL patterns make transformations and validations easier to standardize and review.

#### How to do it step by step

1. Standardize SQL style rules.
2. Use views or curated Gold tables for semantic model exposure.
3. Prefer set-based transformations over row-by-row logic.
4. Validate row counts and reconciliation after major changes.

**Example SQL quality check**

```sql
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT SalesOrderNumber) AS distinct_orders,
    MIN(OrderDate) AS min_order_date,
    MAX(OrderDate) AS max_order_date
FROM gold_sales_daily;
```

#### Effect / gain

- Easier peer review of transformations
- Better maintainability of Gold-layer logic
- Faster troubleshooting during incidents

#### What to avoid

- Hidden transformation logic spread across many undocumented notebooks and scripts
- Business-critical metrics defined differently in multiple SQL objects

---

## Real-Time Intelligence

### 1) Eventstreams, Eventhouse, and KQL

#### Why this matters

**Business rationale**
- Some use cases require telemetry, operational monitoring, or near-real-time decision support.

**Technical rationale**
- Fabric supports real-time ingestion and analysis scenarios through Eventstreams, Eventhouse, and KQL.

#### How to do it step by step

1. Define the event source and desired latency.
2. Ingest events through supported real-time patterns.
3. Store and analyze data in Eventhouse/KQL structures where appropriate.
4. Model downstream aggregates for operational dashboards.
5. Separate exploratory telemetry from governed business KPIs.

**Example KQL**

```kusto
SalesEvents
| where EventTime > ago(1h)
| summarize TotalSales = sum(SalesAmount), Orders = dcount(OrderId) by bin(EventTime, 5m), Region
| order by EventTime asc
```

#### Effect / gain

- Faster operational visibility
- Better alerting and anomaly analysis patterns
- More suitable architecture for streaming or high-frequency event use cases

#### What to avoid

- Forcing all data into real-time architecture when batch analytics is enough
- Mixing lightly governed event data with certified business measures
- Skipping retention and cost planning for event workloads

---

## Security and Governance

### 1) Permission Model

#### Why this matters

**Business rationale**
- Access should follow business roles and least privilege.

**Technical rationale**
- Fabric permission control includes workspace roles, item permissions, compute permissions, and OneLake security patterns.

#### How to do it step by step

1. Map business personas to Fabric roles.
2. Use workspace roles for team-level collaboration control.
3. Use item permissions for controlled sharing.
4. Apply data-level controls where necessary.
5. Document ownership, support scope, and approval rules.

#### Effect / gain

- Lower risk of over-permissioned access
- Cleaner operational model across workspaces and products
- Easier audits and access reviews

#### What to avoid

- Granting broad Member/Admin permissions by default
- Using sharing as a substitute for access design
- Unclear ownership of security decisions

---

### 2) Direct Lake Security, RLS, and OLS

#### Why this matters

**Business rationale**
- Analytics must remain usable while still enforcing business access restrictions.

**Technical rationale**
- Direct Lake security requires alignment between semantic model design, OneLake access, and model-level security logic.

#### How to do it step by step

1. Decide whether access is primarily workspace-based, item-based, or OneLake-based.
2. Add RLS for row-scoped access in semantic models.
3. Add OLS for sensitive columns or tables when needed.
4. Validate the security path for viewers versus contributors.
5. Test with representative users end to end.

**Example RLS filter (semantic model)**

```DAX
'User Access'[UserEmail] = USERPRINCIPALNAME ()
```

#### Effect / gain

- Lower unauthorized exposure risk
- Better compatibility between business governance and self-service consumption
- Cleaner separation between platform access and data access

#### What to avoid

- Assuming report sharing alone secures underlying data
- Implementing RLS without end-to-end testing of the actual data path
- Exposing sensitive columns and trying to hide them only in the UI

---

### 3) Governance Standards

#### Why this matters

**Business rationale**
- Platform scale requires consistent naming, certification, ownership, and lifecycle control.

**Technical rationale**
- Governance reduces duplication, confusion, and unstable downstream dependencies.

#### How to do it step by step

1. Define naming standards for all item types.
2. Define owner, steward, and support contacts for each critical asset.
3. Classify data products by criticality and sensitivity.
4. Establish quality gates for Gold assets and certified semantic models.
5. Review stale or duplicate assets regularly.

#### Effect / gain

- Better catalog quality and discoverability
- Lower support overhead
- More trusted self-service consumption

#### What to avoid

- Allowing anonymous ownership of production data products
- Certifying assets with no operational accountability
- Letting stale semantic models remain visible indefinitely

---

## Performance and Cost Optimization

### 1) Delta and Data Layout Optimization

#### Why this matters

**Business rationale**
- Faster data access improves both engineering throughput and report performance.

**Technical rationale**
- Storage layout, partition strategy, and file hygiene influence query and processing performance.

#### How to do it step by step

1. Avoid excessive small files.
2. Partition large tables intentionally; do not over-partition.
3. Curate Gold tables for common analytics paths.
4. Reconcile latency requirements with update frequency.
5. Validate end-to-end performance from source to report.

#### Effect / gain

- Faster reads and transformations
- Lower processing overhead
- Better experience for downstream semantic models

#### What to avoid

- Tiny-file sprawl from poorly designed write operations
- Partitioning every column “just in case”
- Assuming storage layout never affects reporting performance

---

### 2) Direct Lake and Semantic Model Performance

#### Why this matters

**Business rationale**
- Users judge the platform by report responsiveness.

**Technical rationale**
- Even with Direct Lake, poor model design, high-cardinality dimensions, and inefficient DAX can degrade performance.

#### How to do it step by step

1. Use star schema in the semantic model.
2. Remove unused columns.
3. Reduce high-cardinality text fields where possible.
4. Create reusable base measures.
5. Test report performance with realistic filters and concurrency.
6. Profile slow queries using Power BI tools where applicable.

#### Effect / gain

- Faster visual rendering
- Lower memory pressure
- Better concurrency on shared capacities

#### What to avoid

- Treating Direct Lake as a guarantee that all models will be fast
- Publishing raw wide tables directly to business users
- Leaving unused columns in high-volume semantic tables

---

### 3) Capacity and Operational Efficiency

#### Why this matters

**Business rationale**
- Capacity planning affects both cost and user trust.

**Technical rationale**
- Workload contention can come from engineering jobs, warehouse workloads, semantic models, and real-time analytics sharing the same platform.

#### How to do it step by step

1. Profile major workloads by type and time window.
2. Separate critical workloads if contention becomes material.
3. Schedule heavy transformations outside peak consumption windows when possible.
4. Track baseline performance metrics over time.

**Example scorecard**

```text
Notebook runtime
Pipeline success rate
Gold table load duration
Semantic model query latency
Real-time dashboard latency
Capacity throttling events
```

#### Effect / gain

- Better predictability under load
- More stable user experience
- Stronger cost/performance balance

#### What to avoid

- Running all heavy jobs during peak business reporting hours
- No baseline metrics for platform health
- Attempting optimization without measurement

---

## DevOps, Git Integration, and Deployment Pipelines

### 1) Git Integration

#### Why this matters

**Business rationale**
- Teams need collaboration, review, and rollback capabilities.

**Technical rationale**
- Fabric supports workspace-level Git integration with supported Git providers and supported Fabric item types.

#### How to do it step by step

1. Create separate development workspaces.
2. Connect the workspace to GitHub or Azure DevOps.
3. Define a branching strategy.
4. Commit changes frequently with meaningful messages.
5. Use pull requests for review.
6. Resolve conflicts before promotion.

**Example branch strategy**

```text
main      -> production-aligned
develop   -> integration branch
feature/* -> short-lived change branches
hotfix/*  -> urgent production fixes
```

#### Effect / gain

- Version history for supported items
- Safer team collaboration
- Easier rollback and change auditability

#### What to avoid

- Editing critical assets only in the browser with no version history
- Long-lived feature branches with massive divergence
- Mixing unfinished work directly into the production branch

---

### 2) Deployment Pipelines

#### Why this matters

**Business rationale**
- Predictable promotion reduces release risk.

**Technical rationale**
- Fabric deployment pipelines help move supported items through Dev, Test, and Prod stages.

#### How to do it step by step

1. Create a deployment pipeline.
2. Map Dev, Test, and Prod workspaces.
3. Pair and validate supported items.
4. Apply environment-specific settings where needed.
5. Test data access, refresh, and user scenarios after each promotion.

#### Effect / gain

- Lower release friction
- More consistent promotion process
- Better governance across environments

#### What to avoid

- Manual re-creation of items in higher environments
- Direct edits in production after promotion
- Promoting without smoke tests

---

### 3) ALM Automation and CI/CD

#### Why this matters

**Business rationale**
- Automation reduces operational risk and speeds up delivery.

**Technical rationale**
- Fabric ALM includes Git integration, deployment pipelines, APIs, and variable library capabilities for lifecycle workflows.

#### How to do it step by step

1. Validate naming and structure in pull requests.
2. Enforce code review for notebooks, SQL, and model-related assets.
3. Automate validation where possible.
4. Promote to non-production first.
5. Maintain rollback instructions.
6. Document release notes and operational checks.

**Example pseudo CI/CD workflow**

```yaml
name: Fabric CI

on:
  pull_request:
  push:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate repository structure
        run: echo "Run linting and structure checks"
      - name: Validate SQL and notebooks
        run: echo "Run custom validation scripts"
```

#### Effect / gain

- Earlier detection of packaging and standards issues
- More reliable releases
- Better alignment between engineering and BI delivery

#### What to avoid

- No rollback plan
- No validation gates before production promotion
- Processes dependent on one person’s manual knowledge

---

## Advanced Techniques

### 1) Hybrid Consumption Patterns

#### Why this matters

**Business rationale**
- Enterprises often need both engineering-scale data products and business-friendly semantic models.

**Technical rationale**
- Fabric allows lakehouse, warehouse, notebooks, and Power BI semantic layers to operate together over shared storage patterns.

#### How to do it step by step

1. Keep engineering outputs in governed Gold assets.
2. Expose curated tables to semantic models.
3. Split exploratory and production consumption paths.
4. Define ownership of the data contract between engineering and BI.

#### Effect / gain

- Better collaboration across specialized teams
- More stable business-consumption layer

#### What to avoid

- Letting every consumer query unstable intermediate engineering tables
- Blurring the line between sandbox and production assets

---

### 2) Python and Advanced Notebook Patterns

#### Why this matters

**Business rationale**
- Some scenarios need advanced data science, anomaly detection, or custom feature engineering.

**Technical rationale**
- Python-based notebooks can extend Fabric with advanced algorithms while staying near the data.

#### How to do it step by step

1. Use Python notebooks for scenarios that need more than SQL-only logic.
2. Keep dependencies and runtime assumptions documented.
3. Separate experimentation from productionized notebook paths.
4. Version notebooks through Git integration where supported.

#### Effect / gain

- Better support for advanced analytics scenarios
- Reusable data science patterns near the platform data layer

#### What to avoid

- Productionizing exploratory notebooks without hardening
- Hidden dependency drift between environments

---

### 3) Semantic Model Governance in Fabric

#### Why this matters

**Business rationale**
- A strong semantic layer is still the fastest route to consistent executive reporting.

**Technical rationale**
- Direct Lake, SQL-based serving, and Power BI semantic models require deliberate contracts between upstream and downstream layers.

#### How to do it step by step

1. Define certified Gold tables for semantic consumption.
2. Establish model review and naming standards.
3. Validate DAX, security, and refresh/framing behavior.
4. Document source dependencies and ownership.

#### Effect / gain

- More stable executive reporting
- Lower measure duplication
- Better governance for shared KPIs

#### What to avoid

- Direct business reporting on unstable engineering assets
- No owner for semantic model definitions and KPI logic

---

## Definition of Done Checklist

Use this checklist before promoting a Microsoft Fabric solution to production.

### Platform and Architecture

- [ ] Workspace strategy is defined
- [ ] OneLake domain/layer structure is documented
- [ ] Medallion layers are separated clearly
- [ ] Ownership and support contacts are assigned

### Data Engineering

- [ ] Pipeline orchestration is documented
- [ ] Notebook logic is deterministic and parameterized where appropriate
- [ ] Data quality checks exist for critical datasets
- [ ] Gold tables are analytics-ready

### Semantic Layer

- [ ] Semantic model uses business-friendly naming
- [ ] Star schema is applied where appropriate
- [ ] Date dimension exists and is validated
- [ ] Measures for key KPIs are explicit
- [ ] Security behavior is tested end to end

### Security and Governance

- [ ] Workspace roles reviewed
- [ ] Item permissions reviewed
- [ ] RLS / OLS tested where relevant
- [ ] Sensitive data classification documented

### Performance

- [ ] Gold layer performance validated
- [ ] Direct Lake / semantic model performance tested
- [ ] Operational baselines captured
- [ ] Capacity contention risks reviewed

### DevOps

- [ ] Git integration configured where supported
- [ ] Branching strategy documented
- [ ] Deployment pipeline configured
- [ ] Rollback procedure defined
- [ ] Smoke tests documented

---

## Suggested Naming Standards

| Artifact | Pattern | Example |
|---|---|---|
| Workspace | `<domain>-<environment>` | `finance-prod` |
| Lakehouse | `<domain>_<layer>_lh` | `finance_gold_lh` |
| Warehouse | `<domain>_<layer>_wh` | `finance_gold_wh` |
| Notebook | `<domain>_<purpose>_nb` | `sales_conformance_nb` |
| Pipeline | `<domain>_<flow>_pl` | `sales_ingest_pl` |
| Semantic model | Business-facing name | `Executive Sales Model` |
| Shortcut | `<source>_<purpose>_sc` | `erp_sales_sc` |

---

## Example Measurable Outcomes to Track

Replace the sample figures below with real measurements from your own environment.

| Area | Metric | Example baseline | Example target |
|---|---|---:|---:|
| Ingestion | Pipeline duration | 85 min | 22 min |
| Data engineering | Notebook runtime | 46 min | 14 min |
| Gold serving | Daily aggregate load duration | 31 min | 8 min |
| Semantics | Median visual render time | 2.8 s | 0.8 s |
| Governance | Production incidents per quarter | 9 | 2 |
| Storage | Duplicate copies of key dataset | 4 | 1 |

---

## How to Create the GitHub Repository and Upload This README

Follow these exact steps to create the **`microsoft-fabric-architects-playbook`** repository on GitHub and upload this `README.md` file.

### Option A — Using the GitHub Web Interface

#### Step 1: Sign in to GitHub

1. Go to [https://github.com](https://github.com).
2. Sign in to your account.

#### Step 2: Create a new repository

1. Click the **+** icon in the upper-right corner.
2. Select **New repository**.
3. In **Repository name**, enter:

```text
microsoft-fabric-architects-playbook
```

4. (Optional) Add a short description, for example:

```text
Comprehensive best practices for Microsoft Fabric architecture, governance, performance, and DevOps.
```

5. Choose **Public** or **Private**.
6. You may leave **Add a README file** unchecked, because you will upload your own file.
7. Click **Create repository**.

#### Step 3: Create the README.md file in the repository

1. Inside the new repository, click **Add file**.
2. Choose **Create new file**.
3. In the filename box, type:

```text
README.md
```

4. Copy the full contents of this document.
5. Paste it into the editor.

#### Step 4: Commit the file

1. Scroll to the bottom of the page.
2. In **Commit changes**, enter a message, for example:

```text
Add initial Microsoft Fabric architects playbook README
```

3. Click **Commit changes**.

#### Step 5: Verify the repository

1. Open the repository home page.
2. Confirm that `README.md` renders correctly.
3. Check headings, tables, and code blocks.

---

### Option B — Using Git on Your Local Computer

#### Step 1: Create the repository on GitHub

Repeat **Option A / Step 2** above to create an empty repository named:

```text
microsoft-fabric-architects-playbook
```

#### Step 2: Clone the repository locally

Open a terminal and run:

```bash
git clone https://github.com/<your-user-name>/microsoft-fabric-architects-playbook.git
cd microsoft-fabric-architects-playbook
```

#### Step 3: Create the README.md file

Create a file named `README.md` and paste the content of this document into it.

Example on macOS/Linux:

```bash
touch README.md
```

Example on Windows PowerShell:

```powershell
New-Item README.md -ItemType File
```

#### Step 4: Add, commit, and push

Run:

```bash
git add README.md
git commit -m "Add initial Microsoft Fabric architects playbook README"
git branch -M main
git push -u origin main
```

#### Step 5: Verify on GitHub

1. Refresh the repository page on GitHub.
2. Confirm that `README.md` is visible and rendered.

---

### Optional Next Steps for the Repository

After uploading the README, consider adding:

- `docs/architecture/`
- `docs/security/`
- `docs/governance/`
- `docs/semantic-models/`
- `docs/notebooks/`
- `pipelines/`
- `notebooks/`
- `sql/`
- `workflows/`
- `adr/` (architecture decision records)

---

## References

Official Microsoft Learn references you can use to expand this repository:

- Microsoft Fabric OneLake overview
- Medallion architecture in Fabric
- Lakehouse end-to-end scenario
- Direct Lake overview
- Develop Direct Lake semantic models
- Integrate Direct Lake security
- Fabric permission model
- Fabric Git integration
- Fabric deployment pipelines
- Fabric lifecycle management documentation

---

## Final Note

This README is intentionally pragmatic: centralize data in OneLake where possible, structure it with medallion architecture, publish stable Gold assets, keep semantic models simple, and treat Fabric assets as governed engineering artifacts under source control and lifecycle management.
