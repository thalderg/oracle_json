import json
import codecs
import sys
import cx_Oracle

def connect():
    cur = None
    con = None
    try:
        con = cx_Oracle.connect('TEST/gioben@localhost/PASSW')
        cur = con.cursor()
        extract(cur)
        cur.close()
        con.close()
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        print("Oracle-Error-Code:", error.code)
        print("Oracle-Error-Message:", error.message)
        if cur:
            cur.close()
        if con:
            con.close()


def extract(cur):
    script, table, json_file = sys.argv
    if len(sys.argv) < 3:
        print("no table or json file have been provided!")
        sys.exit(0)
    json_main_dic = {}
    json_list = []
    cur.execute('select * from %s' % table)
    all_fields = get_table_fields(cur, table)
    fields = [field[0] for field in all_fields]
    res = cur.fetchall()
    for data_tp in res:
        json_sub_dic = {}
        for index, key in enumerate(fields):
            json_sub_dic[key] = data_tp[index]
            if json_sub_dic[key] is None: json_sub_dic[key] = ""
            # json_sub_dic[key] = json_sub_dic[key].encode('utf-8').decode()
            json_sub_dic[key] = str(json_sub_dic[key])
        json_list.append(json_sub_dic)
    json_main_dic[table] = json_list
    with open(json_file, 'wb') as fb:
        try:
            json.dump(json_main_dic, codecs.getwriter('utf-8')(fb), sort_keys=True, indent=4, ensure_ascii=False)
        except Exception as e:
            print('error: {}, extracting table {}'.format(e, table))
    print("\nTable {} has been extracted in the below file: \n{}\n".format(table, json_file))


def get_table_fields(cur, table):
    cur.execute('select * from %s' % table)
    # the description is a list of tuples with the following
    #[('EMPLOYEE_ID', < type 'cx_Oracle.NUMBER' >, 7, 22, 6, 0, 0),
    #('FIRST_NAME', < type 'cx_Oracle.STRING' >, 20, 20, 0, 0, 1)]
    return cur.description

if __name__ == '__main__':
    connect()
