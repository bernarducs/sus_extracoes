'''
    Produção de partos.

    Alguns campos:
        N_AIH: numero da aih, é o ID do banco
        GESTAO: 1 municipal, 2 estadual
        MUNIC_RES: munic. residencia
        MUNIC_MOV: munic. internacao
        PROC_REA: procedimento realizado
        ANO_CMPT
        MES_CMPT
        NASC
        SEXO
        DT_INTER
        DT_SAIDA
        IDADE
        MORTE
        CNES
        RACA_COR
        MARCA_UTI: tipo de UTI
        MARCA_UCI: tipo de UCI
'''

import os
import pathlib
import pandas as pd
from pysus.online_data.SIH import download
from pyarrow.lib import ArrowInvalid

STATES = 'pe'
YEAR = 2021
MONTHS = list(range(1, 13))
DATA_DIR = pathlib.Path(os.getcwd(), 'datasets', '.pysus')
PROCED_ID = {
    '0310010039': ['Nornal', 'PARTO NORMAL'],
    '0310010055': ['Normal', 'PARTO NORMAL EM CENTRO DE PARTO NORMAL (CPN)'],
    '0411010034': ['Cesário', 'PARTO CESARIANO'],
    '0411010042': ['Cesário', 'PARTO CESARIANO C/ LAQUEADURA TUBARIA'],
    '0310010047': ['De Risco', 'PARTO NORMAL EM GESTACAO DE ALTO RISCO'],
    '0411010026': ['De Risco', 'PARTO CESARIANO EM GESTACAO ALTO RISCO']
}

os.environ['PYSUS_CACHEPATH'] = str(DATA_DIR)

def extract_files(states, year, months, data_dir):
    files = [download(states, year, m, data_dir) for m in months]
    return files

def _queries_concat(queries):
    q = [
        f"{k} == {v if type(v) == list else [str(v)]}" 
        for k, v in queries.items() 
        ]
        
    query = ' & '.join(q)
    return query

def create_dataframe(data_dir, **kwargs):

    files = os.listdir(data_dir)
    try:
        df = pd.concat(
            [pd.read_parquet(f'{DATA_DIR}/{file}') for file in files]
        )
    except ArrowInvalid:
        err = 'Não foi possível gerar o dataframe '\
            'Os arquivos foram baixados?'
        return print(err)

    query = _queries_concat(kwargs)

    if query:
        return df.query(query)
    return df


if __name__ == '__main__':
    cols_sel = [
        'GESTAO', 
        'MUNIC_RES', 
        'MUNIC_MOV', 
        'PROC_REA', 
        'ANO_CMPT', 
        'MES_CMPT', 
        'NASC',
        'SEXO', 
        'DT_INTER', 
        'DT_SAIDA', 
        'IDADE', 
        'MORTE', 
        'CNES', 
        'RACA_COR', 
        'MARCA_UTI',
        'MARCA_UCI'
    ]

    int_cols = [
        'MUNIC_RES', 
        'MUNIC_MOV',
        'ANO_CMPT', 
        'MES_CMPT', 
        'IDADE', 
        'RACA_COR', 
        'SEXO', 
        'GESTAO'
    ]

    proced = list(PROCED_ID.keys())
    df = create_dataframe(DATA_DIR, PROC_REA=proced)
    df.set_index('N_AIH', inplace=True)
    df_partos = df[cols_sel]
    df_partos[int_cols] = df_partos[int_cols].astype('int32')
    
    df_geres = pd.read_parquet('datasets/localidade_pe.parquet.gzip')
