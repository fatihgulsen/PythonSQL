import urllib
import sqlalchemy as sa
import SqlTransfer
import time

basla1 = time.time()

veritabani = 'SQL_IMPORT'  ## DATABASE NAME

params = "Driver={SQL Server Native Client 11.0};" \
         "Server=localhost\SQLEXPRESS;" \
         f"Database={veritabani};" \
         "Trusted_Connection=yes;"
## VERITABANI ISMI GIRILECEK YER
##SQL EXPRESS VE VERITABANI DEGISTIRELECEK YERLER



sqlexport = SqlTransfer.SqlExport(params)

if __name__ == '__main__':
    sqlexport.transfer(veritabani)
    bit1 = time.time()
    print('süre: %s , genel' % (bit1 - basla1))
