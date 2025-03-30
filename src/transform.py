import os
import json
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def create_dataframe_from_json(json_folder: str) -> pd.DataFrame:
    """
    Lee todos los archivos JSON (raw_data) que se extrajeron de la API de TVMaze y,
    los concatena en un Dataframe de Pandas
    """
    all_dfs = []
    for file_name in os.listdir(json_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(json_folder, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                df_temp = pd.json_normalize(data)  
                all_dfs.append(df_temp)
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
    else:
        df = pd.DataFrame()
    return df


