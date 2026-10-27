# Kyuubi authorization: real plugin, simulated Ranger Admin

| File | Role |
|---|---|
| `ranger-spark-security.xml` | Ranger plugin config loaded by every Kyuubi Spark engine (service name, admin URL, cache dir, poll interval) |
| `ranger-spark-audit.xml` | audit destinations (disabled locally; Solr/HDFS in production) |
| `policies.json` | the access, masking and row-filter policies in Ranger's JSON model (hot-reloaded: bump `policyVersion`) |
| `ranger-servicedef-hive.json` | Ranger's Hive service definition (the AuthZ plugin authorizes Spark SQL against Hive-type policies) |
| `ranger-admin-stub.py` | **LOCAL-ONLY SIMULATION** of the Ranger Admin REST endpoints the plugin polls (`/service/plugins/policies/download/<service>` etc.) |

What is real: the `kyuubi-spark-authz-shaded` jar (Kyuubi 1.12.0, Ranger 2.6 classes) inside the engines, the policy
evaluation, column masking and row filtering. What is simulated: the Ranger Admin server and UI.

## Production configuration (exact)

1. Deploy Ranger Admin 2.8 (`apache/ranger:2.8.0` + `ranger-db` + `ranger-solr` for dev; packaged install for prod)
   with usersync from AD/LDAP so groups `admins`, `etl`, `analysts`, `restricted_analysts` exist.
2. Create a service of type **Hive** named `lakehouse_hive` and import `policies.json`
   (`security/access-policy-examples/ranger-hive-service-policies.json`) through `POST /service/public/v2/api/policy`.
3. On every engine host (or in the engine image) set in `ranger-spark-security.xml`:
   `ranger.plugin.spark.policy.rest.url=https://ranger-admin.<corp>:6182`,
   keep `ranger.plugin.spark.service.name=lakehouse_hive`, point `policy.cache.dir` at a local writable path, and
   enable audit in `ranger-spark-audit.xml` (`xasecure.audit.destination.solr=true`, `...solr.urls=...`).
4. Replace the static group mapping in `infrastructure/hadoop/core-site.xml` with `LdapGroupsMapping` / SSSD.
5. Remove the `ranger-admin-stub` service from the Compose/Kubernetes deployment.

Nothing changes in `spark-defaults.kyuubi.conf` or in Kyuubi: the plugin, extension class and restriction lists are
already the production ones.
