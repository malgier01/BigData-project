from app.library import LibrarySystem
from app.stress_tests import stress_test_1, stress_test_2, stress_test_3
from cassandra.cluster import NoHostAvailable
import time

def wait_and_connect(attempts=12, delay=5):
    db = None
    for attempt in range(attempts):
        try:
            db = LibrarySystem()
            return db
        except NoHostAvailable:
            print(f"Cassandra not ready (attempt {attempt+1}/{attempts}). Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error while connecting to Cassandra: {e}")
            time.sleep(delay)
    return None

if __name__ == "__main__":
    print("Starting Library App... will wait for Cassandra if necessary.")
    db = wait_and_connect()
    if db is None:
        print("Failed to connect to Cassandra after multiple attempts. Exiting.")
        raise SystemExit(1)

    seed = input("Do you want to seed the database? (t/F): ").strip().lower()
    if seed == "t":
        x = input("How many rows to insert? [20]: ")
        n = int(x) if x.isnumeric() else 20
        db.seed(n)

    user = input("Enter username: ").strip() or "guest"
    while True:
        choice = input(
            "\n1: Show all books\n"
            "2: Show your borrowed books\n"
            "3: Borrow a book\n"
            "4: Return a book\n"
            "5: Extend a reservation\n"
            "6: Run stress tests\n"
            "0: Exit\n> "
        ).strip()
        if choice == "1":
            db.display_books()
        elif choice == "2":
            db.display_borrowed_books_by_user(user)
        elif choice == "3":
            book_id = input("Enter book ID to borrow: ").strip()
            db.borrow_book(user, book_id)
        elif choice == "4":
            rid = input("Enter reservation ID to return: ").strip()
            db.return_book(rid, user)
        elif choice == "5":
            rid = input("Enter reservation ID to extend: ").strip()
            db.renew_book(book_id, rid, user)
        elif choice == "6":
            print("\nAvailable Stress Tests:")
            print("1: Same client rapid requests")
            print("2: Randomized multi-client requests")
            print("3: Competing clients fill all reservations")
            t = input("Select test number: ").strip()
            if t == "1":
                stress_test_1(db, user)
            elif t == "2":
                stress_test_2(db)
            elif t == "3":
                stress_test_3(db)
        elif choice == "0":
            print("Goodbye!")
            db.cleanup()
            break
        else:
            print("Unknown option. Try again.")
