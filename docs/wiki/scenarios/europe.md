## Modelling all EU
It is possible to include data for all EU countries by setting the [Upload scenario module](../../reference/plugins/upload_scenario.md) parameters [`tyndp24_scope`](../../reference/plugins/upload_scenario.md/#src.plugins.upload_scenario.nexus_e_plugin.Config.tyndp24_scope) to `"EU"` and [`include_tyndp24`](../../reference/plugins/upload_scenario.md/#src.plugins.upload_scenario.nexus_e_plugin.Config.include_tyndp24) to `true`.

Each new country is modeled as an aggregate single node with aggregate generator capacities. All cross border lines in this case must be defined as NTC connectors.

Further work need to be done to ensure the post process and webviewer allow visualization of results for the full European system.