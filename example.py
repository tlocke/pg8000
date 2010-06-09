from pg8000 import DBAPI

db = DBAPI.connect(
        host="192.168.111.128",
        user="unittest",
        password="unittest",
        database="pg8000")

cursor = db.cursor()
cursor.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT, author TEXT)")

cursor.execute(
        "INSERT INTO book (title, author) VALUES (%s, %s) RETURNING id",
        ("Ender's Game", "Orson Scott Card"))
book_id, = cursor.fetchone()
db.commit()

