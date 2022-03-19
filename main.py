from database_mapper.DB import DB


db = DB(username='postgres', password='postgres', host='127.0.0.1', database='example')

result = db.where(['id', '=', 4]).where(['completed', '=', False]).get(table='table1', columns=['completed'])

for _r in result:
    print(_r[0])