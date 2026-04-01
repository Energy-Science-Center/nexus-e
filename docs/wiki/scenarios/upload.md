Users should use the [Upload scenario module](../../reference/plugins/upload_scenario.md) to define the needed parameters and create new databases using the available TYNDP2024 data and the nodal-hourly loads as well as the EV and HP flexibilities. Two Excel input data files and two folders of input CSV files (use v48 in their names) are available as a starting point to create new databases for simulations. All cross-border connectors should be represented as NTCs.

If the folder /src/data is empty, you need to initialize and update this git submodule:

```bash
git submodule init && git submodule update
```

- [TYNDP24](tyndp24.md)
- [Nodal-hourly load](nodal-hourly-load.md)
- [Cross border connectors as NTCs](ntc.md)
- [Modelling all EU](europe.md)