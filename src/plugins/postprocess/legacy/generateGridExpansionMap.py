import pandas as pd
from datetime import datetime
import os
import subprocess
import glob


def main():
    module_directory = os.path.dirname(os.path.abspath(__file__))

    # Get a list of years
    years = []
    grid_expansion_files = glob.glob(
        os.path.join(
            "Cascades",
            "Cascades_expansion_*.csv"
        )
    )
    for file in grid_expansion_files:
        years.append(file.rsplit(".", 1)[0].rsplit("_", 1)[1])

    for year in years:
        # TODO: enable read from multiple CSVs (e.g. from different years) and generate multiple PNGs.
        cascades_result = pd.read_csv(
            os.path.join(
                "Cascades",
                f"Cascades_expansion_{year}.csv"
            )
        )

        # Add a column "counts" into the dataframe to indicate how many times a line gets expanded
        # Currently the "counts" can only be 1 or 2.
        cascades_result = cascades_result.groupby(['Node_1', 'Node_2']).size().reset_index(name='counts')

        # for years prior to 2025, use the grid map of 2020
        if int(year) < 2025:
            map_year = 2020
        else:
            map_year = 2025

        # Read in relevant CSVs
        line_coordination_df = pd.read_csv(f"{module_directory}/line_coordinations_{map_year}.csv",
                                        index_col=['node1_code', 'node2_code'])
        node_name_id_df = pd.read_csv(f"{module_directory}/node_name_id.csv", index_col=['Node_name'])

        # Generate a temporary grid expansion tex file
        with open(f"{module_directory}/temp_grid_expan.tex", 'w') as f:
            # TODO: also note from which files it is generated.
            f.write(f'% Create on {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}\n')

            for _, cascades_result_row in cascades_result.iterrows():
                node1_name = cascades_result_row[0]
                node2_name = cascades_result_row[1]
                num_of_expansion = cascades_result_row[2]
                if num_of_expansion == 1:
                    highlight_type = "overheadhighlight4"  # purple
                else:
                    highlight_type = "overheadhighlight2"  # orange

                # Find node IDs according to the names
                node1 = node_name_id_df.loc[node1_name]["Node_id"]
                node2 = node_name_id_df.loc[node2_name]["Node_id"]

                # In latex, the order of node1 and node2 might be reversed. Therefore, we try both possibilities.
                # TODO: if there are multiple lines between two nodes, we are not able to distinguish them in the map.
                # The codes below will just return the first entry with the correct node1 and node2.
                # This would be a problem if the lines are not parallel (i.e. with different coordinations, currently not
                # the case).
                try:
                    # This might return multiple entries, if there are multiples (non-parallel) lines between two nodes.
                    line_coordination_row = line_coordination_df.loc[(node1, node2)]
                    coordination = line_coordination_row['coordination'].values[0]
                    node1_tex = line_coordination_row['node1'].values[0]
                    node2_tex = line_coordination_row['node2'].values[0]
                    # This entry below is not exactly correct for transformers - their coordinations are normally written as
                    # "\draw[xxxx] (node1) -- (node2);", i.e. without the "shift" input, but it works.
                    entry = f"\draw[{highlight_type},shift={{({node1_tex})}}] {coordination};\n"
                except:
                    try:
                        line_coordination_row = line_coordination_df.loc[(node2, node1)]
                        coordination = line_coordination_row['coordination'].values[0]
                        node1_tex = line_coordination_row['node1'].values[0]
                        node2_tex = line_coordination_row['node2'].values[0]
                        # This entry below is not exactly correct for transformers - their coordinations are normally
                        # written as "\draw[xxxx] (node1) -- (node2);", i.e. without the "shift" input, but it works.
                        entry = f"\draw[{highlight_type},shift={{({node1_tex})}}] {coordination};\n"
                    except:
                        print(
                            f"ERROR: Line {node1}-{node2} is not found!\n"
                            f"--- Cascades result: Cascades_expansion_{year}.csv\n"
                            f"--- Line coordination csv: line_coordinations_{map_year}.csv"
                        )
                        entry = ""
                        # sys.exit(1)
                f.write(entry)

        ######
        # Generate JPGs from latex
        # (it is faster to render JPG than PNG.)
        ######
        # Change directory to the current directory of this script so that the latex file could find its dependencies
        # (e.g. graphics to include)
        current_directory = os.getcwd()
        try:
            os.chdir(module_directory)

            # TODO: Give a max runtime limit to the following commands. (Note: the solutions are usually platform dependent.)
            # tex->pdf
            # Tested on PSL server, the slash direction in the filepath shouldn't be a problem.
            command = f"pdflatex --enable-write18 {module_directory}/{map_year}_main.tex"
            print(command)
            # specify shell=True to avoid error on windows: "subprocess doesn't look in the PATH unless you pass shell=True"
            # https://stackoverflow.com/questions/3022013/windows-cant-find-the-file-on-subprocess-call
            # To avoid the long output messages (but the output is good for debug), use "capture_output=True"
            subprocess.run(command, shell=True, capture_output=True)
            print(f"Successfully converted to pdf. Now converting to jpg...")

            # pdf->jpg
            map_path = os.path.join("Cascades" f"grid_expansion_map_{year}.jpg")
            command = (
                f"convert {module_directory}/{map_year}_main.pdf" \
                f" -resample 300" \
                f" {map_path}"
            )
            print(command)
            subprocess.run(command, shell=True, capture_output=True)
            print(f"Successfully converted to jpg.")
        finally:
            os.chdir(current_directory)