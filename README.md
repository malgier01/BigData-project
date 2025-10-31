# BigData-project
project done for big data and distributed systems classes

Database schema:
      CREATE TABLE IF NOT EXISTS Books_Library(
                book_id UUID PRIMARY KEY,
                author text,
                title text,
                isbn int
            );""",
            """CREATE TABLE IF NOT EXISTS Reservations_Library(
                book_id UUID,
                reservation_id UUID,
                username text,
                due_date TIMESTAMP,
                PRIMARY KEY (book_id, reservation_id)
            );""",
            """CREATE TABLE IF NOT EXISTS Username_Borrowed (
                book_id UUID PRIMARY KEY,
                username text
            );""",
            """CREATE TABLE IF NOT EXISTS library_locks (
                resource_id UUID PRIMARY KEY,
                username text,
                lock_time TIMESTAMP
            );"""
            CREATE TABLE IF NOT EXISTS Reservations_Library (
                book_id UUID,
                reservation_id UUID,
                username text,
                due_date timestamp,
                PRIMARY KEY (book_id, reservation_id)
            );

