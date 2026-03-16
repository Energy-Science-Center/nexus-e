## Config file
The config.toml file is the interface for the users to configure their simulations.

Here is the default configuration at the time of writing:

```toml
[logging]
filename = "nexus-e.log"
filemode = "w"
format = "%(asctime)s %(levelname)s %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"
level = "INFO"

[results]
base_folder = "Results"
create_new_simulation_results_folder = true

[data_context]
type = "mysql"
name = ""
host = ""
port = ""
user = ""
password = ""

[modules]
playlist_name = "nothing"

[modules.commons]
resolution_in_days = 8
single_electric_node = true
```

You mainly need to:

- fill the `[data_context]` section with credentials for a valid MySQL server and a database `name`. You can find instructions to use Nexus-e team MySQL server [here](https://unlimited.ethz.ch/spaces/WikiNexusE/pages/406917252/MySQL+server).
- select a `playlist_name` corresponding to the name of a file in your `modules_playlists/` folder without the `.toml` extension.

The other elements shouldn't need to be modified for regular nexus-e simulations, some of them are created by nexus-e execution.

## The modules playlists

With this configuration it is possible to define new simulation loops without having to change nexus-e code. The main loops with basic configurations are already available in the folder `modules_playlists/` and you can add new ones in the same folder by following the same structure. The available modules and their parameters are described [here](../../reference/plugins/upload_scenario.md). To use a playlist you need to set the `modules.playlist_name` parameter in the `config.toml` file to corresponds to the file name **without file extension** of a playlist in the `modules_playlists/` folder.