# Enunciado (EN/PT)
# EN: Given a dataset containing a column salary with inconsistent formats 
# (values as strings with currency symbols like "R$3.000,00", shorthand like "3k", 
# different currencies such as "USD 4500" or "€5000", floats, and some missing),
# simulate a SQL → Python → SQL workflow:
# - Use SQL (SQLite in memory) to query the salary column,
# - Clean the column in Python by removing non-numeric characters and handling shorthand formats,
# - Convert all values to floats,
# - Replace missing values with the mean salary per department (conditional imputation),
# - Validate results by removing outliers outside a plausible range (1000–100000),
# - Save the cleaned results back into SQL as a new table.
#
# PT: Dado um conjunto de dados contendo uma coluna salary com formatos inconsistentes 
# (valores como strings com símbolos de moeda, como "R$3.000,00", abreviações como "3k", 
# diferentes moedas como "USD 4500" ou "€5000", floats e alguns ausentes),
# simule um fluxo SQL → Python → SQL:
# - Use SQL (SQLite em memória) para consultar a coluna salary,
# - Limpe a coluna em Python removendo caracteres não numéricos e tratando formatos abreviados,
# - Converta todos os valores para float,
# - Substitua valores ausentes pela média de salário por departamento (imputação condicional),
# - Valide resultados removendo outliers fora de um intervalo plausível (1000–100000),
# - Salve os resultados limpos de volta em SQL como uma nova tabela.

import pandas as pd
import numpy as np
import sqlite3

# Dataset simulado com múltiplos formatos
df = pd.DataFrame({
    'dept': ['HR','IT','HR','IT','Finance'],
    'salary': ['R$3.000,00','USD 4500', None,'3k','€5000']
})

# SQL (entrada): cria banco SQLite em memória e tabela
conn = sqlite3.connect(":memory:")
df.to_sql("funcionarios", conn, index=False, if_exists="replace")

# Consulta SQL
query = "SELECT dept, salary FROM funcionarios"
df_sql = pd.read_sql(query, conn)

# Python (processamento): limpeza + imputação + validação
df_sql['salary'] = df_sql['salary'].astype(str).str.replace(r'[^\dKk]', '', regex=True)
df_sql['salary'] = df_sql['salary'].replace({'3k':'3000','None':np.nan,'':np.nan}).astype(float)
df_sql['salary'] = df_sql.groupby('dept')['salary'].transform(lambda x: x.fillna(x.mean()))
df_sql = df_sql[(df_sql['salary'] >= 1000) & (df_sql['salary'] <= 100000)]

# SQL (saída): salva resultado em nova tabela
df_sql.to_sql("funcionarios_salarios_limpos", conn, index=False, if_exists="replace")

print(df_sql)

