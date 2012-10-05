from pg8000 import dbapi

conn = dbapi.connect('', host='localhost', port=15432, database='myschema')

from pg8000.ext.akiban import extension, AKIBAN_NESTED_CURSOR

conn.enable_extension(extension)
cursor = conn.cursor()

# akiban seems to choke if a trans is in progress, so added an
# "execute_notrans()" method to cursor for now
cursor.execute_notrans("""
CREATE TABLE IF NOT EXISTS customers
  (
     customer_id   INT NOT NULL PRIMARY KEY,
     rand_id       INT,
     name          VARCHAR(20),
     customer_info VARCHAR(100),
     birthdate     DATE
  )
""")

cursor.execute_notrans("""
CREATE TABLE IF NOT EXISTS orders
  (
     order_id    INT NOT NULL PRIMARY KEY,
     customer_id INT NOT NULL,
     order_info  VARCHAR(200),
     order_date  DATETIME NOT NULL,
     GROUPING FOREIGN KEY(customer_id) REFERENCES customers
  )
""")

cursor.execute_notrans("""
CREATE TABLE IF NOT EXISTS items
  (
     item_id  INT NOT NULL PRIMARY KEY,
     order_id INT NOT NULL,
     price    DECIMAL(10, 2) NOT NULL,
     quantity INT,
     GROUPING FOREIGN KEY(order_id) REFERENCES orders
  )
""")

cursor.executemany(
    "INSERT INTO customers VALUES (%s, floor(1 + rand() * 100), %s, %s, %s)",
    [
    (1, 'David McFarlane', 'Co-Founder and CEO', '1982-07-16'),
    (2, 'Ori Herrnstadt', 'Co-Founder and CTO', '1982-07-16'),
    (3, 'Tim Wegner', 'VP of Engineering', '1982-07-16'),
    (4, 'Jack Orenstein', 'Software Engineer', '1982-07-16'),
    (5, 'Peter Beaman', 'Software Engineer', '1982-07-16'),
    (6, 'Thomas Jones-Low', 'Software Engineer', '1982-07-16'),
    (7, 'Mike McMahon', 'Software Engineer', '1982-07-16'),
    (8, 'Padraig O''Sullivan', 'Software Engineer', '1983-12-09'),
    (9, 'Yuval Shavit', 'Software Engineer', '1983-07-05'),
    (10, 'Nathan Williams', 'Software Engineer', '1984-05-01'),
    (11, 'Chris Ernenwein', 'Software Testing Engineer', '1982-07-16'),
    ]
)

cursor.executemany(
    "INSERT INTO orders VALUES(%s, %s, %s, date_sub(now(), floor(1 + rand() * 100)))",
    [
    (101, 1, 'apple related', ),
    (102, 1, 'apple related', ),
    (103, 1, 'apple related', ),
    (104, 2, 'kite', ),
    (105, 2, 'surfboard', ),
    (106, 2, 'some order info', ),
    (107, 3, 'some order info', ),
    (108, 3, 'some order info', ),
    (109, 3, 'some order info', ),
    (110, 4, 'some order info', ),
    (111, 4, 'some order info', ),
    (112, 4, 'some order info', ),
    (113, 5, 'some order info', ),
    (114, 5, 'some order info', ),
    (115, 5, 'some order info', ),
    (116, 6, 'some order info', ),
    (117, 6, 'some order info', ),
    (118, 6, 'some order info', ),
    (119, 7, 'some order info', ),
    (120, 7, 'some order info', ),
    (121, 7, 'some order info', ),
    (122, 8, 'some order info', ),
    (123, 8, 'some order info', ),
    (124, 8, 'some order info', ),
    (125, 9, 'some order info', ),
    (126, 9, 'some order info', ),
    (127, 9, 'some order info', ),
    (128, 10, 'some order info', ),
    (129, 10, 'some order info', ),
    (130, 10, 'some order info', ),
    (131, 11, 'some order info', ),
    (132, 11, 'some order info', ),
    (133, 11, 'some order info', ),
    ])

cursor.executemany(
    "INSERT INTO items VALUES (%s, %s, %s, %s)",
    [
        (1001, 101, 9.99, 1),
        (1002, 101, 19.99, 2),
        (1003, 102, 9.99, 1),
        (1004, 103, 9.99, 1),
        (1005, 104, 9.99, 5),
        (1006, 105, 9.99, 1),
        (1007, 106, 9.99, 1),
        (1008, 107, 999.99, 1),
        (1009, 107, 9.99, 1),
        (1010, 108, 9.99, 1),
        (1011, 109, 9.99, 1),
    ]
)

def printrows(cursor, indent=""):
    for row in cursor.fetchall():
        nested = []
        out = ""
        for field, col in zip(cursor.description, row):
            if field[1] == AKIBAN_NESTED_CURSOR:
                nested.append((field[0], col, indent))
            else:
                out += " " + str(col)
        print indent + out
        for key, values, indent in nested:
            printrows(values, "%s    %s: " % (indent, key))

cursor.execute("select 2 as Y, 1 as X, 3 as Z, (select 4 as NX1) as X1, "
                "(select 5 as NY1) AS Y1")
printrows(cursor)

cursor.execute("""
    select customers.*,
           (select orders.*,
                (select items.*
                from items
                where items.order_id = orders.order_id and
                orders.customer_id = customers.customer_id) as items
            from orders
            where orders.customer_id = customers.customer_id) as orders
    from customers
""")

printrows(cursor)
