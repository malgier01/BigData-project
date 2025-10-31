# app/library.py

from cassandra.cluster import Cluster, NoHostAvailable, BatchStatement
from cassandra import OperationTimedOut, WriteTimeout, ReadTimeout
from datetime import datetime, timedelta, timezone
import pandas as pd
import uuid
import logging
import time

logging.basicConfig(level=logging.INFO)

class LibrarySystem:
    def __init__(self, hosts=None, retries=12, wait=5):
        self.hosts = hosts or [('127.0.0.1', 9042), ('127.0.0.1',9043), ('127.0.0.1', 9044)]
        self.cluster = None
        self.session = None

        for attempt in range(retries):
            try:
                logging.info(f"Connecting to Cassandra hosts: {self.hosts}")
                
                self.cluster = Cluster(self.hosts, protocol_version=5, port=9042)
                print(self.cluster)
                self.session = self.cluster.connect()
                
                try:
                    self.session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS reservations
                        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};
                    """)
                except Exception as e:
                    logging.warning(f"Keyspace creation failed (node may not be ready): {e}")
                    time.sleep(wait)
                    continue

                self.session.set_keyspace('reservations')
                logging.info("Connected to Cassandra and keyspace ready.")
                break
            except NoHostAvailable:
                logging.warning(f"Cassandra not ready (attempt {attempt+1}/{retries}). Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                print(e)
                logging.error(f"Unexpected error: {e}")
                print(e)
                time.sleep(wait)
        else:
            raise ConnectionError("Failed to connect to Cassandra cluster after several attempts.")

        # self._create_tables()
        self._prepare_statements()
        logging.info("LibrarySystem initialized successfully.")


    def _prepare_statements(self):
        self.insert_book = self.session.prepare(
            "INSERT INTO Books_Library (book_id, author, title, isbn) VALUES (?, ?, ?, ?)"
        )
        self.select_all_books = self.session.prepare("SELECT * FROM Books_Library")
        self.select_book_by_id = self.session.prepare(
            "SELECT * FROM Books_Library WHERE book_id = ?"
        )
        self.insert_lock = self.session.prepare("""
            INSERT INTO library_locks (resource_id, username, lock_time)
            VALUES (?, ?, ?)
            IF NOT EXISTS USING TTL 30
        """)
        self.select_lock_owner = self.session.prepare(
            "SELECT username FROM library_locks WHERE resource_id = ?"
        )
        self.delete_lock = self.session.prepare(
            "DELETE FROM library_locks WHERE resource_id = ?"
        )
        self.insert_reservation = self.session.prepare("""
            INSERT INTO Reservations_Library (book_id, reservation_id, username, due_date)
            VALUES (?, ?, ?, ?)
        """)
        self.insert_reservation_by_user = self.session.prepare("""
            INSERT INTO Reservations_By_User (book_id, reservation_id, username, due_date)
            VALUES (?, ?, ?, ?)
        """)
        self.select_reservations_by_user = self.session.prepare(
            "SELECT * FROM Reservations_Library WHERE username = ?"
        )
        self.select_reservation_details = self.session.prepare(
            # "SELECT * FROM Reservations_Library WHERE username = ? AND reservation_id = ?"
            "SELECT * FROM Reservations_By_User WHERE username = ? AND reservation_id = ?",
            print('dupa')
        )
        self.update_reservation_due_date = self.session.prepare(
            # "UPDATE Reservations_Library SET due_date = ? WHERE book_id = ? AND reservation_id = ?"
            "UPDATE Reservations_Library SET due_date = ? WHERE book_id = ? AND reservation_id = ?"
            # "UPDATE Reservations_Library SET due_date = ? WHERE reservation_id = ?"


        )
        self.delete_reservation = self.session.prepare(
            "DELETE FROM Reservations_Library WHERE book_id = ? AND reservation_id = ?"
        )
        self.insert_borrowed = self.session.prepare(
            "INSERT INTO Username_Borrowed (book_id, username) VALUES (?, ?)"
        )
        self.select_borrowed = self.session.prepare(
            "SELECT * FROM Username_Borrowed WHERE book_id = ?"
        )
        self.delete_borrowed = self.session.prepare(
            "DELETE FROM Username_Borrowed WHERE book_id = ?"
        )
        logging.info("Prepared all Cassandra statements.")

    def seed(self, n=20):
        """Seed the database with books from books.csv"""
        for table in ["Books_Library", "Reservations_Library", "Username_Borrowed", "library_locks"]:
            self.session.execute(f"TRUNCATE {table}")

        books = pd.read_csv("/home/malgier/Pulpit/BigData_project/app/books.csv", usecols=["book_id", "authors", "title"])
        books["author"] = books["authors"].str.split(",").str[0]
        books.drop(columns=["authors"], inplace=True)
        books.dropna(inplace=True)

        batch = BatchStatement()
        for _, row in books.sample(n).iterrows():
            batch.add(self.insert_book, (uuid.uuid4(), row["author"], row["title"], 0))
        self.session.execute(batch)
        logging.info(f"Seeded {n} books into Books_Library.")

    
    # Locking & Borrowing Functions

    def acquire_lock(self, book_id, username):
        now = datetime.now(timezone.utc)
        result = self.session.execute(self.insert_lock, (uuid.UUID(book_id), username, now))
        return result.was_applied

    def borrow_book(self, username, book_id):
        try:
            book_uuid = uuid.UUID(book_id)
        except ValueError:
            print("Invalid book ID")
            return

        if self.session.execute(self.select_borrowed, [book_uuid]).one():
            print("Book already borrowed")
            return

        if not self.acquire_lock(book_id, username):
            print("Book is currently locked")
            return

        owner = self.session.execute(self.select_lock_owner, [book_uuid]).one()
        if not owner or owner.username != username:
            print("Book is not available")
            return

        batch = BatchStatement()
        due_date = datetime.now(timezone.utc) + timedelta(days=30)
        reservation_id = uuid.uuid4()
        print(reservation_id)
        batch.add(self.insert_reservation, (book_uuid, reservation_id, username, due_date))
        batch.add(self.insert_reservation_by_user, (book_uuid, reservation_id, username, due_date))
        batch.add(self.insert_borrowed, (book_uuid, username))
        batch.add(self.delete_lock, (book_uuid,))
        self.session.execute(batch)
        print(f"Book borrowed successfully by {username}")

    def display_books(self):
        rows = list(self.session.execute(self.select_all_books))
        print(f"\n{'Book ID':36} | {'Author':20} | {'Title':40}")
        print("-" * 100)
        for row in rows:
            print(f"{str(row.book_id):36} | {row.author[:20]:20} | {row.title[:40]:40}")
        print()

    def display_borrowed_books_by_user(self, username):
        rows = list(self.session.execute(self.select_reservations_by_user, [username]))
        if not rows:
            print(f"{username} has no borrowed books.")
            return
        print(f"\nBorrowed books for {username}\n")
        print(f"{'Reservation ID':36} | {'Title':30} | {'Due Date':10}")
        print("-" * 80)
        for row in rows:
            book = self.session.execute(self.select_book_by_id, [row.book_id]).one()
            if book:
                due = row.due_date.strftime("%d/%m/%Y")
                print(f"{str(row.reservation_id):36} | {book.title[:30]:30} | {due:10}")

    def renew_book(self, book_id, reservation_id, username):
        reservation = self.session.execute(
            self.select_reservation_details, (username, uuid.UUID(reservation_id))
        ).one()
        if not reservation:
            # print(reservation)
            print("Reservation not found")
            return
        new_due = reservation.due_date + timedelta(days=30)
        self.session.execute(
            self.update_reservation_due_date, (new_due, uuid.UUID(book_id), uuid.UUID(reservation_id))
        )
        print(f"Renewed until {new_due.date()}")

    def return_book(self, reservation_id, username):
        
        reservation = self.session.execute(
            self.select_reservation_details, (username, uuid.UUID(reservation_id))
        ).one()
        if not reservation:
            print("Reservation not found")
            return
        batch = BatchStatement()
        batch.add(self.delete_reservation, (reservation.book_id, reservation.reservation_id))
        batch.add(self.delete_borrowed, (reservation.book_id,))
        self.session.execute(batch)
        print(f"Returned book {reservation.book_id}")
