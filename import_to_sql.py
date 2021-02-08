import SqlTransfer
import time

basla1 = time.time()

veritabani = 'SQL_IMPORT'  ## DATABASE NAME
server_name = 'localhost\SQLEXPRESS'  ## SERVER NAME

params = "Driver={SQL Server Native Client 11.0};" \
         f"Server={server_name};" \
         f"Database={veritabani};" \
         "Trusted_Connection=yes;"

#  VERITABANI ISMI GIRILECEK YER
# SQL EXPRESS VE SQL_IMPORT DEGISTIRELECEK YERLER

dosya_dir = r'C:\Users\fatih\Desktop\PythonSQL'  # DOSYA KAYNAGI
# OKUMA DOSYASI

sqlimport = SqlTransfer.SqlImport(params)

if __name__ == '__main__':
    sqlimport.transfer(dosya_dir)
    bit1 = time.time()
    print('süre: %s , genel' % (bit1 - basla1))
