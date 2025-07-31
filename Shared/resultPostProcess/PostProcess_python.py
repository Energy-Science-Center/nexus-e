import argparse
import subprocess
import os

def run_script(script_name, simu_name, db_name):
    command = [
        'python', script_name,
        f'--simuname={simu_name}',
        f'--DBname={db_name}'
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}: {e}")

def upload_to_mysql(simu_name, scen_name, version_wv):
    command = [
        'python', 'moveToMysql.py',
        f'--simu-name={simu_name}',
        f'--scen-name={scen_name}',
        f'--version-wv={version_wv}'
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"FAILED to upload results to the output database: {e}")

def main():
    argp = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argp.add_argument(
        "--simuname",
        type=str,
        help="Name of MySQL database results",
        default='pathfndr_s8_241119_cpv_s8'
    )
    argp.add_argument(
        "--DBname",
        type=str,
        help="Name of MySQL database",
        default='pathfndr_s8_241119_cpv_s8'
    )
    argp.add_argument(
        "--batch",
        type=int,
        help="Batch mode flag",
        default=0
    )
    argp.add_argument(
        "--processMysql",
        type=int,
        help="Process MySQL flag",
        default=0
    )
  
    argp.add_argument(
        "--version-wv",
        type=str,
        help="Version WV",
        default='results'
    )
    args = argp.parse_args()
    simu_name = args.simuname
    database = args.DBname
    batch = args.batch
    process_mysql = args.processMysql
    scen_name = database
    version_wv = args.version_wv

    scripts = [
        "demand.py",
        "Generation.py",
        "Curtailments.py",
        "Capacity.py",
        "Storage.py",
        "ElectricityPrice.py",
        "revenue_profit.py",
        "system_costs.py",
        "emissions.py",
        "cross_country_flow.py",
        "power_flow_map.py"
    ]

    for script in scripts:
        print(f"Executing {script}...")
        run_script(script, simu_name, database)

    # Upload post-process results to output database
    if not batch or process_mysql == 1:
        print('---------------- MySQL -----------------')
        try:
            command = [
                'python', 'moveToMysql.py',
                f'--simu-name={simu_name}',
                f'--scen-name={scen_name}',
                f'--version-wv={version_wv}'
            ]
            print(' '.join(command))
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print("FAILED to upload results to the output database.")
        finally:
            os.chdir(os.path.dirname(__file__))

    print("Post-process finished.")

if __name__ == "__main__":
    main()