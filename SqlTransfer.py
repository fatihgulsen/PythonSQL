import math
import pandas as pd
import os
import sqlalchemy as sa
import pyodbc
import time
import urllib


class SqlImport:
    def __init__(self, _params):
        self.params = urllib.parse.quote_plus(_params)

        _engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        self.engineFast = _engine

        _engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, )
        self.engineSlow = _engine

    @staticmethod
    def __sqlcol(data):
        dtypedict = {}
        for i, j in zip(data.columns, data.dtypes):
            if "object" in str(j):
                if data[i].str.len().max() <= 255:
                    # print(data[i].name, data[i].str.len().max())
                    dtypedict.update({i: sa.types.NVARCHAR(length=255)})
                else:
                    # print(data[i].name, data[i].str.len().max())
                    dtypedict.update({i: sa.types.NVARCHAR})
            elif "datetime" in str(j):
                dtypedict.update({i: sa.types.DateTime()})

            elif "float" in str(j):
                dtypedict.update({i: sa.types.Float})

            elif "int" in str(j):
                dtypedict.update({i: sa.types.Float})
        return dtypedict

    @staticmethod
    def __chunker(seq, size):
        return (seq[pos: pos + size] for pos in range(0, len(seq), size))

    def transfer(self, _dir):
        dosya_list = self.__read_dir(_dir)
        for dosya in dosya_list:
            dosya_boyut = os.path.getsize(dosya)
            print(f'Dosya boyutu : {dosya_boyut}')
            if dosya.endswith('.xlsx'):
                basla1 = time.time()

                data = pd.read_excel(dosya)
                dosya = dosya.replace('.xlsx', '')
                dosya = dosya.replace(' ', '')
                dtypes_dict = self.__sqlcol(data)
                try:
                    data.to_sql(dosya, con=self.engineFast, if_exists='replace', index=False, dtype=dtypes_dict)
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
                        if dosya_boyut < 512000000:
                            data.to_sql(name=dosya + '_' + table_name, con=self.engineFast, if_exists='replace',
                                        index=False,
                                        dtype=dtypes_dict)
                            print(f'Access hızlı aktarıldı : {dosya}_{table_name}')

                        else:
                            SQL_SERVER_CHUNK_LIMIT = 2099
                            chunksize = math.floor(SQL_SERVER_CHUNK_LIMIT / len(data.columns))

                            for chunk in self.__chunker(data, chunksize):
                                chunk.to_sql(
                                    name=dosya + '_' + table_name, con=self.engineSlow, if_exists='append',
                                    index=False,
                                    dtype=dtypes_dict
                                )
                            print(f'Access yavas aktarıldı : {dosya}_{table_name}')

                    except:
                        print(f'Dosya aktarılamadı : {dosya}_{table_name}')
                    bit1 = time.time()
                    print(f'süre: %s , {dosya}' % (bit1 - basla1))
        pass

    @staticmethod
    def __read_dir(_dir):
        os.chdir(_dir)
        dosya_list = os.listdir()
        excel_access_list = [i for i in dosya_list if i.endswith('.xlsx') or i.endswith('.accdb') or i.endswith('.mdb')]
        return excel_access_list
        pass


class SqlExport:
    def __init__(self, _params):
        params = urllib.parse.quote_plus(_params)

        _engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)
        self.engine = _engine
        pass

    def transfer(self, _veritabani, col_len=25):
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
                        try:
                            for _col in data.columns:
                                if _col == 'IMPORTER_COUNTRY' or _col == 'N_IMPORTER_NAME':
                                    data.drop(columns=_col, axis=1, inplace=True)
                        except:
                            pass

                        column = data.columns

                        for col in column:
                            try:
                                data[col] = data[col].str.replace(';', ',')
                                data[col] = data[col].str.replace(r'^=+', ' ')
                                data[col] = data[col].str.replace(r'[\n\r\t]', ' ')
                                data[col] = data[col].str.strip()
                            except:
                                pass

                        try:
                            with pd.ExcelWriter("%s.xlsx" % table, datetime_format='dd.mm.yyyy hh:mm:ss',
                                                date_format='dd.mm.yyyy', engine='xlsxwriter') as writer:

                                data.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
                                workbook = writer.book
                                worksheet = writer.sheets['Sheet1']
                                (max_row, max_col) = data.shape
                                column_settings = [{'header': column} for column in data.columns]
                                worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
                                worksheet.set_column(0, max_col - 1, col_len)
                                writer.save()
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
