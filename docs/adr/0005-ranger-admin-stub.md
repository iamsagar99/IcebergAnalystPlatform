# 0005. Real Ranger plugin, simulated Ranger Admin (local only)

Date: 2026-10-26. Status: accepted for the POC; not a production option.

## Context

Kyuubi's authorization comes from the Kyuubi Spark AuthZ plugin, which embeds the Apache Ranger plugin and polls
Ranger Admin for policies. Ranger Admin 2.8 with its database and Solr does not fit the 7.5 GB Docker budget next
to Trino, Kyuubi and three Spark engines.

## Decision

Run the real plugin inside every engine, pointed at a 120-line HTTP stub that serves the Ranger policy-download
endpoints from `policies.json` and the Hive service definition. Label it as a simulation everywhere.

## Consequences

* Policy evaluation, masking and row filtering are the real Ranger code paths; only the admin server/UI is missing.
* Switching to production is one URL change plus importing the same JSON into Ranger Admin.
* The stub strips Ranger-UI date formats that the shaded Jackson parser rejects, and answers 304 when the
  version is unchanged, so engine logs look like a real deployment.
