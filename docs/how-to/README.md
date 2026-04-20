# How-to Guides

Task-oriented guides. Each guide states its preconditions, shows the steps, and names the outcome.

If you are on-call responding to a page, go to [runbooks/](../runbooks/README.md) — those are the incident-specific procedures with the 8-section shape. If you need a comprehensive lookup for a flag or metric, go to [reference/](../reference/).

## Index

| Guide                                           | Task                                                            |
| ----------------------------------------------- | --------------------------------------------------------------- |
| [Deployment](deployment.md)                     | Cluster setup, bootstrap modes, backup, restore.                |
| [Logging](logging.md)                           | Enable structured logs; query canonical log lines.              |
| [Profiling](profiling.md)                       | Capture flamegraphs; investigate a performance regression.      |
| [Observability cost](observability-cost.md)     | Control metric and log ingestion costs on paid vendors.         |
| [Troubleshooting](troubleshooting.md)           | Symptom index routing to the appropriate runbook or fix.        |
| [Capacity planning](capacity-planning.md)       | Size a cluster; estimate resource growth.                        |

## Related

- [getting-started/](../getting-started/) — first-time tutorial.
- [runbooks/](../runbooks/) — incident-response procedures.
- [playbooks/](../playbooks/) — scheduled maintenance procedures.
