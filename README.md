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

How to run:
docker: docker-compose up --build
app: python3 -m app.main

Stress tests:
1 - rapid same request - book borrowed only once
2 - randomized multi client request - 12.17s for 5000 randomized requests
3 - competing clients - due to the CAP theorem there still exist some discrepancies in the borrowed books
