import os
import pandas as pd
from pysus.online_data.SIH import download

STATES = 'pe'
YEAR = 2021
MONTHS = list(range(1, 13))
DATA_DIR = '/home/berna/microdata/sus/datasets/sih'
PROCED_ID = {
    '0310010039': ['Nornal', 'PARTO NORMAL'],
    '0310010055': ['Normal', 'PARTO NORMAL EM CENTRO DE PARTO NORMAL (CPN)'],
    '0411010034': ['Cesário', 'PARTO CESARIANO'],
    '0411010042': ['Cesário', 'PARTO CESARIANO C/ LAQUEADURA TUBARIA'],
    '0310010047': ['De Risco', 'PARTO NORMAL EM GESTACAO DE ALTO RISCO'],
    '0411010026': ['De Risco', 'PARTO CESARIANO EM GESTACAO ALTO RISCO']
}

def extract_files(states, year, m, data_dir):
    files = [download(STATES, YEAR, m, DATA_DIR) for m in MONTHS]
    return files

def create_dataframe(data_dir, proced_id, cd_munic, cd_gestao):
    files = os.listdir(data_dir)
    df = pd.concat(
        [pd.read_parquet(f'{DATA_DIR}/{file}') for file in files]
    )
    query = 'PROC_REA == {} and ' \
        'MUNIC_RES == "{}" and ' \
        'GESTAO == "{}"'.format(proced_id, cd_munic, cd_gestao)

    return df.query(query)
