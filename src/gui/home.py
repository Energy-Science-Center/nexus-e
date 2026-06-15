import numpy as np
import pandas as pd
import streamlit as st
import time

from nexus_e_interface.scenario import Scenario, DataContext
import nexus_e_interface.tables as nexus_e_tables

def main():
    conn = st.connection("prod")
    databases = conn.query(
        "SHOW databases"
    )

    with st.sidebar:
        database = st.selectbox(
            label="Scenario",
            options=databases,
        )
        st.button(
            label="Run simulation"
        )
    
    tables, config, playlist = st.tabs(
        [
            "Tables",
            "Config",
            "Playlist",
        ]
    )

    scenario = Scenario(
        DataContext(
            type=st.secrets.connections.prod.dialect,
            name=database,
            host=st.secrets.connections.prod.host,
            port="3306",
            user=st.secrets.connections.prod.username,
            password=st.secrets.connections.prod.password,
        )
    )

    # scenario.get

    with tables:
        table_name = st.selectbox(
            label="Table",
            options=[
                p
                for p in dir(Scenario)
                if isinstance(getattr(Scenario,p),property)
                if not p.startswith("_")
            ],
            placeholder="dbinfo"
        )
        original_table = pd.DataFrame(getattr(scenario, table_name))
        edited_table = st.data_editor(
            data = original_table.drop("_sa_instance_state", axis=1)
        )

    
    

if __name__ == "__main__":
    main()