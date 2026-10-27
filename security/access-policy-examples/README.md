# Production policy examples

The POC enforces the model with Trino file rules and Ranger JSON served by a stub. In production the same model is
expressed in a central policy service:

* `ranger-hive-service-policies.json` - the Ranger policies for the Kyuubi/Spark engines (identical content to
  `infrastructure/kyuubi/authz/policies.json`; import into Ranger Admin with the REST API or UI). Service type: Hive.
* `ranger-trino-policies.json` - the same model for the **Ranger Trino plugin** (`access-control.name=ranger`).
* `opa-trino.rego` - the same model as an **Open Policy Agent** policy for Trino's OPA access control
  (`access-control.name=opa`, `opa.policy.uri=http://opa:8181/v1/data/trino/allow`), including column masks and row filters.

Pick Ranger if the client already runs Ranger for HDFS/Hive (one UI, one audit store, group sync from AD);
pick OPA if policies are managed as code and the Kyuubi path is not in production.
