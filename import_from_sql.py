import os
import sqlalchemy as sa
import time
import urllib
import SqlTransfer

basla1 = time.time()

params = "Driver={SQL Server Native Client 11.0};" \
         "Server=localhost\SQLEXPRESS;" \
         "Database=SQL_IMPORT;" \
         "Trusted_Connection=yes;"
## VERITABANI ISMI GIRILECEK YER
##SQL EXPRESS VE SQL_IMPORT DEGISTIRELECEK YERLER

dosya_dir = r'C:\Users\fatih\Desktop\PythonSQL'  # DOSYA KAYNAGI
# OKUMA DOSYASI

params = urllib.parse.quote_plus(params)

_engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)
sqlimport = SqlTransfer.SqlImport(_engine)

os.chdir(dosya_dir)
dosya_list = os.listdir()
excel_access_list = []
for i in dosya_list:
    if i.endswith('.xlsx') or i.endswith('.accdb') or i.endswith('.mdb'):
        excel_access_list.append(i)

if __name__ == '__main__':
    sqlimport.transfer(excel_access_list)
    bit1 = time.time()
    print('süre: %s , genel' % (bit1 - basla1))
