from pathlib import Path

def get_years_simulated_by_centiv(results_path: Path) -> list[int]:
    centiv_folders: list[str] = results_path.glob("CentIv_*")
    output = [int(str(folder).split("_")[-1]) for folder in centiv_folders]
    output = sorted(output)
    return output