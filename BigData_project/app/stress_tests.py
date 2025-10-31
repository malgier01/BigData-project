from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter
import random
import uuid
import logging

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("stress_tests")


def stress_test_1(db, username="tester"):
    LOG.info("Running Stress Test 1 — rapid same-request spam...")
    books = db.session.execute(db.select_all_books).all()
    if not books:
        print("No books available for testing.")
        return
    book_id = str(books[0].book_id)

    NUM_REQUESTS = 5000
    MAX_WORKERS = 16

    start = perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(db.borrow_book, username, book_id) for _ in range(NUM_REQUESTS)]
        for f in as_completed(futures):
            pass
    duration = perf_counter() - start
    LOG.info(f"Test 1 completed in {duration:.2f}s for {NUM_REQUESTS} requests.")
    db.cleanup()


def stress_test_2(db):
    LOG.info("Running Stress Test 2 — randomized concurrent clients...")
    books = db.session.execute(db.select_all_books).all()
    if not books:
        print("No books available for testing.")
        return

    usernames = [f"user_{i}" for i in range(1, 11)]
    MAX_WORKERS = 20
    NUM_REQUESTS = 5000

    def random_action():
        user = random.choice(usernames)
        book = random.choice(books)
        book_id = str(book.book_id)
        try:
            action = random.choice(["borrow", "return", "renew"])
            if action == "borrow":
                db.borrow_book(user, book_id)
            elif action == "return":
                reservations = list(db.session.execute(db.select_reservations_by_user, [user]))
                if reservations:
                    rid = str(random.choice(reservations).reservation_id)
                    db.return_book(rid, user)
            elif action == "renew":
                reservations = list(db.session.execute(db.select_reservations_by_user, [user]))
                if reservations:
                    rid = str(random.choice(reservations).reservation_id)
                    db.renew_book(book_id, rid, user)
        except Exception as e:
            LOG.error(f"Error in random action: {e}")

    start = perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(random_action) for _ in range(NUM_REQUESTS)]
        for f in as_completed(futures):
            pass
    duration = perf_counter() - start
    LOG.info(f"Test 2 completed in {duration:.2f}s for {NUM_REQUESTS} randomized requests.")
    db.cleanup()


def stress_test_3(db):
    LOG.info("Running Stress Test 3 — 2 users competing for all books...")
    books = db.session.execute(db.select_all_books).all()
    if not books:
        print("No books available for testing.")
        return

    def occupy_books(username):
        for book in books:
            db.borrow_book(username, str(book.book_id))

    start = perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(occupy_books, "alice")
        executor.submit(occupy_books, "bob")

    duration = perf_counter() - start
    alice_borrowed = len(list(db.session.execute(db.select_reservations_by_user, ["alice"])))
    bob_borrowed = len(list(db.session.execute(db.select_reservations_by_user, ["bob"])))

    LOG.info(f"Test 3 completed in {duration:.2f}s.")
    LOG.info(f"Alice borrowed {alice_borrowed} books.")
    LOG.info(f"Bob borrowed {bob_borrowed} books.")
    db.cleanup()
