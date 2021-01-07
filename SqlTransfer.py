import pandas as pd
import os
import sqlalchemy as sa
import pyodbc
import time


class SqlImport:
    def __init__(self, engine):
        self.engine = engine

    def __sqlcol(self, data):
        dtypedict = {}
        for i, j in zip(data.columns, data.dtypes):
            if "object" in str(j):
                dtypedict.update({i: sa.types.NVARCHAR(length=255)})
            elif "datetime" in str(j):
                dtypedict.update({i: sa.types.DateTime()})

            elif "float" in str(j):
                dtypedict.update({i: sa.types.Float})

            elif "int" in str(j):
                dtypedict.update({i: sa.types.Float})
        return dtypedict

    def transfer(self, dosya_list):
        for dosya in dosya_list:
            if dosya.endswith('.xlsx'):
                basla1 = time.time()

                data = pd.read_excel(dosya)
                dosya = dosya.replace('.xlsx', '')
                dosya = dosya.replace(' ', '')
                dtypes_dict = self.__sqlcol(data)
                try:
                    data.to_sql(dosya, con=self.engine, if_exists='replace', index=False, dtype=dtypes_dict)
                    print(f'Excel  aktarıldı : {dosya}')

                except:
                    print(f'Dosya aktarılamadı : {dosya}')
                bit1 = time.time()
                print(f'süre: %s ,{dosya}' % (bit1 - basla1))
            elif dosya.endswith('.accdb') or dosya.endswith('.mdb'):
                basla1 = time.time()
                klasor = os.getcwd()
                conn_string = (r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
                               r"DBQ=%s\\%s;" % (klasor, dosya))

                conn = pyodbc.connect(conn_string)
                table_name = ''
                crsr = conn.cursor()

                for table_info in crsr.tables(tableType='TABLE'):
                    table_name = table_info.table_name
                    data = pd.read_sql_query('select * from %s' % table_name, conn)
                    dtypes_dict = self.__sqlcol(data)
                    if dosya.endswith('.accdb'):
                        dosya = dosya.replace('.accdb', '')
                        dosya = dosya.replace(' ', '')
                    elif dosya.endswith('.mdb'):
                        dosya = dosya.replace('.mdb', '')
                        dosya = dosya.replace(' ', '')

                    try:
                        data.to_sql(name=dosya + '_' + table_name, con=self.engine, if_exists='replace', index=False,
                                    dtype=dtypes_dict)
                        print(f'Access aktarıldı : {dosya}_{table_name}')

                    except:
                        print(f'Dosya aktarılamadı : {dosya}_{table_name}')
                    bit1 = time.time()
                    print(f'süre: %s , {dosya}' % (bit1 - basla1))

        pass


class SqlExport:
    def __init__(self, engine):
        self.engine = engine
        pass

    def transfer(self, _veritabani):
        sql_table = pd.read_sql_query('SELECT * FROM ' + _veritabani + '.sys.tables', self.engine)
        sql_table = sql_table['name']

        for table in sql_table:
            if table.startswith('p1') or table.startswith('p7') or table.startswith('P7') or table.startswith('P1'):
                basla1 = time.time()
                count = f'SELECT DISTINCT count(*) FROM [{table}]'
                count_df = pd.read_sql_query(count, self.engine)
                _count = count_df.iloc[0, count_df.columns.get_loc('')]
                if _count <= 1048576:
                    query = f'SELECT DISTINCT * FROM [{table}]'
                    try:
                        data = pd.read_sql_query(query, self.engine)
                        column = data.columns

                        for col in column:
                            try:
                                data[col] = data[col].str.replace(';', ',')
                                data[col] = data[col].str.replace(r'^=+', ' ')
                                data[col] = data[col].str.strip()
                            except:
                                pass

                        with pd.ExcelWriter("%s.xlsx" % table, datetime_format='dd.mm.yyyy hh:mm:ss',
                                            date_format='dd.mm.yyyy') as writer:
                            try:
                                data.to_excel(writer, index=False)
                                print('%s ciktisi Alindi' % table)
                            except:
                                print('Excel Cikti Hatasi')

                        bit1 = time.time()
                        print(f'süre: %s , {table}' % (bit1 - basla1))
                        del data, column, writer, col, query, table

                    except:
                        print(f" Okuma Hatasi : {table}")

                else:
                    print('Satir sayisi excel icin fazla %s' % table)
            else:
                print('p1,p7 ile baslamiyor %s' % table)

        del self.engine, sql_table
        pass
