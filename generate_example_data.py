import sqlite3
import random
import pathlib
from datetime import datetime, timedelta

conn = sqlite3.connect(pathlib.Path(__file__).parent / 'library.db')
cur = conn.cursor()

cur.execute("DELETE FROM checkouts")

cur.execute("SELECT id FROM students")
student_ids = [row[0] for row in cur.fetchall()]

cur.execute("SELECT rowid FROM books")
book_ids = [row[0] for row in cur.fetchall()]

if not student_ids or not book_ids:
    print("Error: No students or books found in database. Please import students and books first.")
    conn.close()
    exit(1)

print(f"Found {len(student_ids)} students and {len(book_ids)} books")

today = datetime.today()
checkout_data = []
books_currently_out = set()
student_current_checkouts = {}

print("Generating historical checkout data...")
for _ in range(len(book_ids) * 50):
    book_id = random.choice(book_ids)
    student_id = random.choice(student_ids)
    
    days_ago = random.randint(30, 1095)
    checkout_date = (today - timedelta(days=days_ago)).date()
    
    return_days = random.randint(7, 30)
    return_date = checkout_date + timedelta(days=return_days)
    
    checkout_data.append((student_id, book_id, checkout_date.strftime('%Y-%m-%d'), return_date.strftime('%Y-%m-%d')))

print("Generating additional varied historical data...")
for _ in range(len(student_ids) * 20):
    book_id = random.choice(book_ids)
    student_id = random.choice(student_ids)
    
    days_ago = random.randint(90, 730)
    checkout_date = (today - timedelta(days=days_ago)).date()
    
    return_days = random.randint(5, 45)
    return_date = checkout_date + timedelta(days=return_days)
    
    checkout_data.append((student_id, book_id, checkout_date.strftime('%Y-%m-%d'), return_date.strftime('%Y-%m-%d')))

print("Generating current checkout data...")
available_books = book_ids.copy()
random.shuffle(available_books)

for book_id in available_books:
    if len(books_currently_out) >= len(book_ids) * 0.3:
        break
        
    available_students = [s for s in student_ids if student_current_checkouts.get(s, 0) < 2]
    if not available_students:
        break
        
    student_id = random.choice(available_students)
    
    days_ago = random.randint(0, 29)
    checkout_date = (today - timedelta(days=days_ago)).date()
    
    books_currently_out.add(book_id)
    student_current_checkouts[student_id] = student_current_checkouts.get(student_id, 0) + 1
    
    checkout_data.append((student_id, book_id, checkout_date.strftime('%Y-%m-%d'), None))

print(f"Inserting {len(checkout_data)} checkout records...")
cur.executemany("INSERT INTO checkouts (student_id, book_id, checkout_date, return_date) VALUES (?, ?, ?, ?)", checkout_data)

conn.commit()
conn.close()

historical_count = len([c for c in checkout_data if c[3] is not None])
current_count = len([c for c in checkout_data if c[3] is None])
print(f"Generated {len(checkout_data)} total checkout records:")
print(f"  - {historical_count} historical checkouts (returned)")
print(f"  - {current_count} current checkouts (not returned)")
print(f"  - {len(books_currently_out)} books currently checked out")
print(f"  - Students with current checkouts: {len([s for s, count in student_current_checkouts.items() if count > 0])}")
print("Checkout data generation complete!")