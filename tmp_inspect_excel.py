import os
import pandas as pd

for name in ['2x2 online.xlsx', '2x2 presencial.xlsx']:
    path = os.path.join(os.getcwd(), name)
    print('FILE', name, os.path.exists(path))
    if os.path.exists(path):
        xl = pd.ExcelFile(path)
        print('SHEETS', xl.sheet_names)
        for s in xl.sheet_names:
            df = pd.read_excel(path, sheet_name=s)
            print('SHEET', s, df.shape)
            print(df.head(30).to_string(index=False))
            print('---')
