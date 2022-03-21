from database_mapper.DB import DB

db = DB(username='postgres', password='postgres', host='127.0.0.1', database='example')

try:
    db.begin_transaction()

    db.insert('table1', {
        'id': 5,
        'completed': True
    })

    db.insert('table1', {
        'id': 7,
        'completed': False
    })

    db.commit_transaction()

except:
    db.rollback_transaction()

result = db.where(['completed', '=', False]).get(table='table1', columns=['completed'])

for _r in result:
    print(_r[0])
