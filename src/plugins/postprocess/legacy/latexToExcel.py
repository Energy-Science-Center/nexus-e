import re
import pandas as pd
import os

def main():
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # for simulations of 2020, use the map of 2020; for simulations of after 20205, use the map of 2025.
    years = [2020, 2025]

    for year in years:
        file = open(f"{current_directory}/{year}_lines_nodes.tex", "r")
        lines_all = file.readlines()

        # %% ######################
        # to create a table of
        # | code | name |
        # note: this table is different from node_name_id.csv because this tables uses the names from the latex file,
        # while node_name_id.csv uses the names from the database, i.e. the names in the cascades results.
        ############################
        code_and_name = pd.DataFrame()
        for line in lines_all:
            if line.startswith(r"\node"):  # to simply filter out some lines (saving computation time)
                # sometimes the "\node" lines don't contain all nodes, e.g. the node "AllAcqua-Y",
                # but we can't use the "\coordinate" lines because they don't contain the codes, but only the names.
                regex = re.compile(r'^\\node\[.*\] \((?P<code>.*)\) at \((?P<name>.*)\)')
                results = regex.search(line)
                results_dict = results.groupdict()

                # Calibrate with Nexus_GridData_For2020thru2050_DISAGG_Nuc50_v24 from Jared
                # Note: some nodes have been removed for 2025: e.g. CH_Visp_Y1_220 (SY/VIS2A). Such nodes are shown in
                # Jared's excel but not in the excel generated from this script.

                # 1. for swiss nodes, add an "A" at the end if the code ends with a number, e.g. SAATHA2 -> SAATHA2A
                if re.search(r'^S.*\d$', results_dict['code']):
                    results_dict['code'] = results_dict['code'] + "A"
                # 2. for swiss nodes starting with "SY_", change the underscore to slash, e.g. SY_GAB2A -> SY/GAB2A
                if re.search(r'^SY_', results_dict['code']):
                    results_dict['code'] = results_dict['code'].replace("_", "/", 1)

                code_and_name = code_and_name.append(results_dict, ignore_index=True)
        # make the name column as the index, to facilitate search in the dataframe
        code_and_name = code_and_name.set_index('name')

        # Individual changes
        code_and_name.at['ch_y_weiach_220', 'code'] = 'SY/WEI2A'  # previously: SY_EGL2
        code_and_name.at['ch_y_laufenburg_220', 'code'] = 'SLAUFU2B'  # previously: SY_LFB2
        code_and_name.at['ch_birr_220', 'code'] = 'SBIRR 2A'  # previously: SBIRR_2
        code_and_name.at['ch_gabi_220', 'code'] = 'SGABI 2A'  # previously: SGABI_2
        code_and_name.at['ch_nant_de_dran_380', 'code'] = 'SNDRAN1A'  # previously: SNANT_1
        code_and_name.at['ch_riet_220', 'code'] = 'SRIET 2A'  # previously: SRIET 2A
        code_and_name.at['ch_sils_380', 'code'] = 'SSILS 1A'  # previously: SSILS_1
        code_and_name.at['ch_sils_220', 'code'] = 'SSILS 2A'  # previously: SSILS_2
        code_and_name.at['ch_st_triphon_220', 'code'] = 'SS.TRI2A'  # previously: SST_TR2
        code_and_name.at['ch_vaux_220', 'code'] = 'SVAUX 2A'  # previously: SVAUX_2
        code_and_name.at['ch_auswiesen_220', 'code'] = 'SAUWIE2A'  # previously: SAUSWI2
        code_and_name.at['ch_fionnay_ffm_220', 'code'] = 'SFIONF2A'  # previously: SFIONN2
        code_and_name.at['ch_gyrnau_220', 'code'] = 'SGRYNA2A'  # previously: SGYRNA2

        if year == 2020:
            # This was only a Y-node, not a substation in year 2020, so we have to manually add it.
            code_and_name.loc['ch_all_acqua_220', 'code'] = 'SACQUA2A'

        # %%
        # save
        code_and_name.to_csv(f"{current_directory}/node_code_and_name_{year}.csv")

        # %% ######################
        # to create a table of
        # | node1_code | node2_code | node1 | node2 | line_coordinations |
        ############################
        line_coordinations = pd.DataFrame()
        for line in lines_all:
            # (1) Find the coordinations of overhead lines
            # use the if-else to simply filter out some lines (saving computation time)
            if line.startswith(r"\draw[overhead220") or line.startswith(r"\draw[overhead380") \
                    or line.startswith(r"\draw[overhead150"):
                regex = re.compile(
                    r'^\\draw\[overhead.*,shift={\((?P<node1>.*)\)}\] (?P<coordination>.*\((?P<node2>.*)\));'
                )
                results = regex.search(line)
                # add node codes based on the names using the code_and_name dataframe generated above
                results_dict = results.groupdict()
                results_dict['node1_code'] = code_and_name.loc[results.group('node1')]['code']
                results_dict['node2_code'] = code_and_name.loc[results.group('node2')]['code']

                line_coordinations = line_coordinations.append(results_dict, ignore_index=True)

            # (2) Find the coordinations of transformers
            elif line.startswith(r"\draw[OneMark"):
                regex = re.compile(
                    r'^\\draw\[OneMark.*\] (?P<coordination>\((?P<node1>.*)\).*\((?P<node2>.*)\));'
                )
                results = regex.search(line)
                # add node codes based on the names using the code_and_name dataframe generated above
                results_dict = results.groupdict()
                results_dict['node1_code'] = code_and_name.loc[results.group('node1')]['code']
                results_dict['node2_code'] = code_and_name.loc[results.group('node2')]['code']

                line_coordinations = line_coordinations.append(results_dict, ignore_index=True)


        # make the code column as the index, to facilitate search in the dataframe
        line_coordinations.drop_duplicates(inplace=True)  # remove duplicated rows if all values in those rows are the same.
        line_coordinations = line_coordinations.set_index(['node1_code', 'node2_code'])

        # %%
        # save (reorder columns)
        line_coordinations = line_coordinations[['node1', 'node2', 'coordination']]
        line_coordinations.to_csv(f"{current_directory}/line_coordinations_{year}.csv")

if __name__ == "__main__":
    main()