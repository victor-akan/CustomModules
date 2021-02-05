import pandas as pd
import cx_Oracle

# def getcon():
#     un="data_analyst"
#     pwd="dataguy888"
#     db_uri="dwdrscan.gtbank.com:1521/odidb"
#     return cx_Oracle.connect(un, pwd, db_uri)

# con = getcon()
# res = pd.read_sql_query('select * from dual', con)
# print(res)
# con.close()

def get_conn_object():
    un="data_analyst"
    pwd="dataguy888"
    db_uri="dwdrscan.gtbank.com:1521/odidb"
    return cx_Oracle.connect(un, pwd, db_uri)
    
def does_table_exists(table_name):
    table_name = table_name.upper()
    ret = False
    con = get_conn_object()
    cursor = con.cursor()
    try:
        # df = self.read_df_from_db(f"select * from user_tables where upper(table_name) = '{table_name}' ")
        query = f"select * from {table_name}"
        cursor.execute(query)
        print(cursor.fetchone())
        ret = bool(cursor.fetchone())
    except :
        pass
    finally:
        cursor.close()
        con.close()
    return ret

print(does_table_exists('AIR_ETL_GROCERIES'))
print(does_table_exists('AIR_ETL_AIRTIME_TRANSACT'))

