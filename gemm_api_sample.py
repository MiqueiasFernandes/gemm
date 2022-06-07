## pip install flask flask-cors
## export FLASK_APP=postgres_sample && flask run

import os
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import psycopg2

app = Flask(__name__)
CORS(app)

### ### ### ### ### DADOS DE AMOSTRA ### ### ### ### ### 
DIM_PRODUTO = """       DIM_PRODUTO   ;  NOME        ;  VALOR
                        1             ;  Tv          ;  1890,78
                        2             ;  Computador  ;  3400,90
                        3             ;  Smartphone  ;  700,90
"""
DIM_CLIENTE = """       DIM_CLIENTE   ;   NOME
                        1             ;   Jose
                        2             ;   Maria
"""

FT_VENDA = """          FT_VENDA    ; DIM_CLIENTE   ; DIM_PRODUTO ; QUANTIDADE
                        1           ; 1             ; 3           ; 4
                        2           ; 2             ; 1           ; 9
                        3           ; 1             ; 2           ; 5
"""
DADOS = {
    "DIM_PRODUTO":  [[y.strip() for y in x.split(';')] for x in  DIM_PRODUTO.splitlines()],
    "DIM_CLIENTE":  [[y.strip() for y in x.split(';')] for x in  DIM_CLIENTE.splitlines()],
    "FT_VENDA":     [[y.strip() for y in x.split(';')] for x in  FT_VENDA.splitlines()]
}


### ### ### ### ### DADOS DE AMOSTRA: CSV arquivo unico ### ### ### ### ### 
LOCAL_ARQUIVO = './dados_gemm_api.csv'

try: open(LOCAL_ARQUIVO)  ### Criar arquivo de amostra se ele não existir
except FileNotFoundError:
    pd.DataFrame(
        [['DIM_PRODUTO'] + x + [''] for x in DADOS['DIM_PRODUTO']] +
        [['DIM_CLIENTE'] + x + ['', ''] for x in DADOS['DIM_CLIENTE']] +
        [['FT_VENDA'] + x for x in DADOS['FT_VENDA']]
    ).to_csv(LOCAL_ARQUIVO, sep=';', decimal=',', header=None, index=None)

df = pd.read_csv(LOCAL_ARQUIVO, sep=';', header=None, dtype=str)
DADOS = {t: [list(x) for x in df[df[0] == t].iloc[:,1:].dropna(axis=1, how='all').values] for t in set(df[0])}


### ### ### ### ### DADOS DE AMOSTRA: CSV arquivo separado ### ### ### ### ### 
for k, v in DADOS.items():
    try: open(k + '.csv')  ### Criar arquivo de amostra se ele não existir
    except FileNotFoundError:
        open(k+'.csv', 'w').writelines([';'.join(x)+'\n' for x in v])

TABELAS = ['DIM_PRODUTO', 'DIM_CLIENTE', 'FT_VENDA']
DADOS = {t: [list(x) for x in pd.read_csv(t + '.csv', delimiter=';', header=None, dtype=str).values] for t in TABELAS}
dt_c = DADOS


### ### ### ### ### DADOS DE AMOSTRA: XLSX ### ### ### ### ### 
TABELAS = ['DIM_PRODUTO', 'DIM_CLIENTE', 'FT_VENDA']
LOCAL_ARQUIVO = './cubo_vendas.xlsx'
DADOS = {s: [list(x) for x in pd.read_excel(LOCAL_ARQUIVO, sheet_name=s, header=None, dtype=str).values] for s in TABELAS}


### DADOS DE AMOSTRA: Postgres SQL
def get_db_connection():
    conn = psycopg2.connect(host='hh-pgsql-public.ebi.ac.uk',
                           database='pfmegrnargs',
                           user='reader',
                           password='NWDMCE5xdipIjRrp')
    return conn

SQL = { 
    'DIM_PRODUTO': ' UNION ALL '.join(["SELECT "+", ".join([f"'{x[i]}' {DADOS['DIM_PRODUTO'][0][i]}" 
                                          for i in range(len(x))]) +" " for x in DADOS['DIM_PRODUTO'][1:]]),
    'DIM_CLIENTE': ' UNION ALL '.join(["SELECT "+", ".join([f"'{x[i]}' {DADOS['DIM_CLIENTE'][0][i]}" 
                                          for i in range(len(x))]) +" " for x in DADOS['DIM_CLIENTE'][1:]]),
    'FT_VENDA': ' UNION ALL '.join(["SELECT "+", ".join([f"'{x[i]}' {DADOS['FT_VENDA'][0][i]}" 
                                          for i in range(len(x))]) +" " for x in DADOS['FT_VENDA'][1:]])
}

def get_results(SQL):
    dx = {}
    with get_db_connection() as conn:
        for tb, query in SQL.items():
            with conn.cursor() as cursor:
                cursor.execute(query)
                dt = [[x[0].upper() for x in cursor.description]]
                dt.extend([list(row) for row in cursor])
                dx[tb] = dt
    return dx
DADOS = get_results(SQL)

### VALIDAR SE LAYOUT DADOS ESTA CERTO
for tb, vals in DADOS.items():
    assert tb == vals[0][0]
    
print("Tabelas:", ', '.join(DADOS)) 
    
@app.route('/list')
def list():
    return jsonify( { k: len(v)-1 for k, v in DADOS.items() } )

@app.route('/DATA/<tabela>')
def get(tabela):
    return jsonify(DADOS[tabela])
