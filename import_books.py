import pandas as pd
import sqlite3
import pathlib

df = pd.read_csv(
    'library export DLR_25f02.csv',
    quotechar='"',
    doublequote=True, 
)

df_selected = df[[
    'Local Number', 'Title', 'Sub Title', 'Author(s)', 
    'Call 1', 'Call 2', 'Publisher', 'Published', 'ISBN #', 'Location'
]].copy()

df_selected.columns = [
    'localnumber', 'title', 'subtitle', 'author', 
    'call1', 'call2', 'publisher', 'published', 'isbn', 'booklocation'
]

try:
    db_path = pathlib.Path(__file__).parent / 'library.db'
    conn = sqlite3.connect(str(db_path))
except:
    print("Failure")

df_selected.to_sql('books', conn, index=False, if_exists='replace')
conn.close()