import time
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

sqlimport = SqlTransfer.SqlImport(params)

excel_access_list = sqlimport.read_dir(dosya_dir)

if __name__ == '__main__':
    sqlimport.transfer(excel_access_list)
    bit1 = time.time()
    print('süre: %s , genel' % (bit1 - basla1))
