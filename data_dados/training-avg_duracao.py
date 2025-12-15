### Enunciado (EN/PT)  
#  EN: Given a table of international veterinary training sessions with inconsistent "duration" values (some in minutes, some in hours), normalize all durations to hours, calculate the average duration per country, and store the aggregated dataset back into SQL.  
# PT: Dada uma tabela de sessões internacionais de treinamento veterinário com valores inconsistentes de "duração" (alguns em minutos, outros em horas), normalize todas as durações para horas, calcule a média de duração por país e grave o conjunto de dados agregado novamente em SQL.  

### Solução (SQL → Python → SQL, máx. 10 linhas)  

import sqlite3, pandas as pd
con = sqlite3.connect(":memory:")
con.execute("CREATE TABLE training(id INTEGER, country TEXT, duration REAL, unit TEXT)")
con.executemany("INSERT INTO training VALUES(?,?,?,?)",
                [(1,"Brazil",90,"min"),(2,"Brazil",2,"h"),
                 (3,"USA",120,"min"),(4,"USA",3,"h")])
df = pd.read_sql("SELECT * FROM training", con)
df['duration'] = df.apply(lambda r: r['duration']/60 if r['unit']=="min" else r['duration'], axis=1)
agg = df.groupby('country', as_index=False)['duration'].mean()
agg.to_sql("training_avg", con, if_exists="replace", index=False)
print(pd.read_sql("SELECT * FROM training_avg", con))
