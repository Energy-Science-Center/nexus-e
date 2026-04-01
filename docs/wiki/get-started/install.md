## Prerequisite
You will need the following software to work with Nexus-e:

- [MATLAB 2023b](https://mathworks.com/products/new_products/release2023b.html) (needed only for multi-year runs, skip otherwise - license needed) with the following toolboxes:
    - Database Toolbox
    - Financial Toolbox
    - Communication Toolbox
- [MySQL](https://dev.mysql.com/doc/mysql-getting-started/en/)
- [MySQL Workbench](https://dev.mysql.com/doc/workbench/en/wb-installing.html) (recommended - to visually interact with data)
- [Python](https://www.python.org/) directly if you don't want to use UV (see below)
- [Gurobi](https://www.gurobi.com/) (license needed on the device running the optimization - free Academic license can be ordered if running on personal device is needed)

We advise to also use:

- [UV](https://docs.astral.sh/uv/getting-started/installation/) (python projects manager)
- [git](https://git-scm.com/download) (version control system)
- an IDE like [VS Code](https://code.visualstudio.com/)

### SQL connector for MATLAB
Download the [MySQL connector](https://dev.mysql.com/downloads/connector/j/). Unzip the file. Before downloading, select “Platform Independent” from the “Select Operating System” drop-down list.

Copy the MySQL connector folder (e.g. mysql-connector-java-8.0.18) into a folder at your preference. We recommend to put it into the Matlab preferences folder, which you can find by typing in Matlab Command Window `prefdir`.

Create a `javaclasspath.txt` file in the Matlab preferences folder.

In the `javaclasspath.txt` file, write the path to the connector .jar file that you just copied. E.g., On a Windows computer the path is similar to C:\Users\user\AppData\Roaming\MathWorks\MATLAB\R2018a\mysql-connector-java-8.0.18\mysql-connector-java-8.0.18.jar.

Reload Matlab

Test whether a database connector is set up successfully. Write the following commands in Matlab with the following substitutions:

- YOUR_SERVER: the MySQL server you want to use
- YOUR_USERNAME: valid username of the chosen MySQL server
- YOUR_PASSWORD: corresponding password

```matlab
conn = database("sys", YOUR_USERNAME, YOUR_PASSWORD, 'Vendor', 'MySQL', 'Server', 'YOUR_SERVER');
isopen(conn)
```

If the second command returns 1, it means success.

## Setup Nexus-e
Clone the nexus-e repository by running the command:

```bash
git clone git@github.com:Energy-Science-Center/nexus-e.git
```

If you want to also download the data we use to produce our open-source scenarios you need to run instead:

```bash
git clone --recursive git@github.com:Energy-Science-Center/nexus-e.git
```

## Running Nexus-e
You can proceed with running Nexus-e with our [python workflow](run.md).
Note: if you would like to connect to Euler, please consider [connecting via ssh using VS Code](../how-to/work-on-euler.md).