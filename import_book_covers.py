#!/usr/bin/env python3
"""
Book Cover Import Script for CCA Library

This script downloads book covers from Open Library API and stores them in the database.
Uses asyncio for concurrent downloads while respecting rate limits.

Usage:
    python import_book_covers.py [database_path]

Features:
- Async concurrent downloads (10 at a time) for 5-10x speed improvement
- Rate limiting (overall ~25 covers/sec max)
- Only downloads covers for books with ISBNs that don't already have covers
- Stores covers as BLOB data in the database
- Progress tracking and error handling
- Can be stopped and resumed (only processes books without covers)
"""

import argparse
import asyncio
import pathlib
import sqlite3
import sys
import time
from io import BytesIO

import aiohttp

# Concurrency settings
MAX_CONCURRENT_DOWNLOADS = 10  # Number of simultaneous downloads
REQUEST_TIMEOUT = 10  # seconds
OVERALL_RATE_LIMIT = 0.04  # 0.04s = 25 requests/second max (spread across concurrent downloads)

# Open Library API settings
COVER_API_URL = "https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
USER_AGENT = "CCALibrary/1.0 (Library Management System; cover download script)"


def add_cover_column_if_needed(db_path):
    """Add cover_image column to books table if it doesn't exist"""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Check if column exists
    cur.execute("PRAGMA table_info(books)")
    columns = [row[1] for row in cur.fetchall()]
    
    if 'cover_image' not in columns:
        print("Adding cover_image column to books table...")
        cur.execute("ALTER TABLE books ADD COLUMN cover_image BLOB")
        conn.commit()
        print("✓ Column added successfully")
    else:
        print("✓ cover_image column already exists")
    
    conn.close()


def get_books_needing_covers(db_path):
    """Get list of unique ISBNs that need covers, grouped by ISBN"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get unique ISBNs and the books that share them
    # Exclude books that have been processed (have cover_image data)
    cur.execute("""
        SELECT isbn, GROUP_CONCAT(rowid) as rowids, 
               MIN(title) as title, MIN(localnumber) as localnumber
        FROM books
        WHERE isbn IS NOT NULL 
        AND isbn != ''
        AND cover_image IS NULL
        GROUP BY isbn
        ORDER BY MIN(rowid)
    """)
    
    isbn_groups = cur.fetchall()
    conn.close()
    
    # Convert to list of dicts with rowids as list
    result = []
    for group in isbn_groups:
        result.append({
            'isbn': group['isbn'],
            'title': group['title'],
            'localnumber': group['localnumber'],
            'rowids': [int(x) for x in group['rowids'].split(',')]
        })
    
    return result


def clean_isbn(isbn):
    """Clean ISBN by removing dashes and spaces"""
    if not isbn:
        return None
    return isbn.replace('-', '').replace(' ', '').strip()


async def download_cover(session, isbn, semaphore, rate_limiter):
    """Download cover image from Open Library API with async"""
    clean = clean_isbn(isbn)
    if not clean:
        return None
    
    url = COVER_API_URL.format(isbn=clean)
    
    async with semaphore:  # Limit concurrent connections
        await rate_limiter()  # Respect overall rate limit
        
        try:
            headers = {'User-Agent': USER_AGENT}
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT), headers=headers) as response:
                # Open Library returns a 1x1 placeholder image for missing covers
                # Check content length to avoid storing placeholder
                if response.status == 200:
                    content = await response.read()
                    if len(content) > 1000:
                        return content
                return None
                
        except Exception as e:
            print(f"    ⚠ Request failed: {e}")
            return None


def save_cover_to_db(db_path, rowids, cover_data):
    """Save cover image to database for all books with the same ISBN"""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Update all books with the same ISBN
    placeholders = ','.join('?' * len(rowids))
    cur.execute(f"""
        UPDATE books
        SET cover_image = ?
        WHERE rowid IN ({placeholders})
    """, (cover_data, *rowids))
    
    conn.commit()
    conn.close()


def mark_no_cover(db_path, rowids):
    """Mark books as processed even though no cover was found"""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    # Store a tiny marker (single byte) to indicate we've tried
    # This prevents retrying books that don't have covers available
    marker = b'NO_COVER'
    
    placeholders = ','.join('?' * len(rowids))
    cur.execute(f"""
        UPDATE books
        SET cover_image = ?
        WHERE rowid IN ({placeholders})
    """, (marker, *rowids))
    
    conn.commit()
    conn.close()


async def rate_limiter_factory():
    """Factory for creating a rate limiter function with request tracking"""
    last_request_time = 0
    lock = asyncio.Lock()
    request_times = []  # Track request timestamps for rate calculation
    
    async def limiter():
        nonlocal last_request_time
        async with lock:
            current_time = time.time()
            time_since_last = current_time - last_request_time
            if time_since_last < OVERALL_RATE_LIMIT:
                await asyncio.sleep(OVERALL_RATE_LIMIT - time_since_last)
            last_request_time = time.time()
            
            # Track this request
            request_times.append(time.time())
            # Keep only requests from the last second
            cutoff = time.time() - 1.0
            while request_times and request_times[0] < cutoff:
                request_times.pop(0)
    
    async def get_current_rate():
        async with lock:
            return len(request_times)
    
    limiter.get_current_rate = get_current_rate
    return limiter


async def download_batch(books, db_path, progress_callback=None):
    """Download a batch of covers concurrently"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    rate_limiter = await rate_limiter_factory()
    
    successful = 0
    failed = 0
    books_updated = 0
    completed_count = 0
    total = len(books)
    lock = asyncio.Lock()
    
    # Rate monitoring task
    async def monitor_rate():
        while True:
            await asyncio.sleep(1.0)
            rate = await rate_limiter.get_current_rate()
            if rate > 0 and not progress_callback:
                print(f"    📊 Current rate: {rate} requests/second")
    
    async def process_and_track(session, book, db_path, semaphore, rate_limiter):
        """Process a book and update progress"""
        nonlocal successful, failed, books_updated, completed_count
        
        result = await process_book(session, book, db_path, semaphore, rate_limiter, progress_callback)
        
        async with lock:
            completed_count += 1
            if result:
                successful += result['successful']
                failed += result['failed']
                books_updated += result['books_updated']
            
            # Update progress callback with actual completion count
            if progress_callback:
                num_books = len(book['rowids'])
                duplicate_note = f" ({num_books} books)" if num_books > 1 else ""
                progress_callback(completed_count, total, f"{book['title'][:50]}{duplicate_note}")
        
        return result
    
    async with aiohttp.ClientSession() as session:
        # Start rate monitoring
        monitor_task = asyncio.create_task(monitor_rate())
        
        tasks = []
        for book in books:
            task = process_and_track(session, book, db_path, semaphore, rate_limiter)
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Stop monitoring
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    
    return {
        'successful': successful,
        'failed': failed,
        'books_updated': books_updated
    }


async def process_book(session, book, db_path, semaphore, rate_limiter, progress_callback):
    """Process a single book (download and save)"""
    try:
        num_books = len(book['rowids'])
        
        if not progress_callback:
            print(f"ISBN: {book['isbn']}")
        
        # Download cover
        cover_data = await download_cover(session, book['isbn'], semaphore, rate_limiter)
        
        result = {'successful': 0, 'failed': 0, 'books_updated': 0}
        
        if cover_data:
            # Save to database
            save_cover_to_db(db_path, book['rowids'], cover_data)
            size_kb = len(cover_data) / 1024
            if not progress_callback:
                print(f"    ✓ Cover downloaded ({size_kb:.1f} KB) - updated {num_books} book(s)")
            result['successful'] = 1
            result['books_updated'] = num_books
        else:
            # Mark as attempted
            mark_no_cover(db_path, book['rowids'])
            if not progress_callback:
                print(f"    ✗ No cover available - marked {num_books} book(s) as processed")
            result['failed'] = 1
        
        if not progress_callback:
            print()
        
        return result
    except Exception as e:
        print(f"    Error processing book: {e}")
        return {'successful': 0, 'failed': 1, 'books_updated': 0}


def import_covers(db_path, limit=None):
    """Main function to import book covers"""
    print("=" * 70)
    print("CCA Library - Book Cover Import Script (Async)")
    print("=" * 70)
    print()
    
    # Ensure cover_image column exists
    add_cover_column_if_needed(db_path)
    print()
    
    # Get books that need covers
    print("Fetching books that need covers...")
    books = get_books_needing_covers(db_path)
    
    if not books:
        print("✓ All books with ISBNs already have covers!")
        return
    
    total = len(books)
    if limit:
        books = books[:limit]
        print(f"Found {total} books needing covers (processing {len(books)} due to limit)")
    else:
        print(f"Found {total} books needing covers")
    
    print()
    print(f"Concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")
    print(f"Rate limit: ~{1/OVERALL_RATE_LIMIT:.0f} requests/second max")
    print(f"Estimated time: {(len(books) * OVERALL_RATE_LIMIT / 60):.1f} minutes (with concurrency)")
    
    # Calculate total books affected
    total_books = sum(len(b['rowids']) for b in books)
    print(f"Total books to update: {total_books} (from {len(books)} unique ISBNs)")
    print()
    print("Starting download... (Press Ctrl+C to stop)")
    print("-" * 70)
    
    start_time = time.time()
    
    try:
        # Run async download
        result = asyncio.run(download_batch(books, db_path))
        successful = result['successful']
        failed = result['failed']
        books_updated = result['books_updated']
    except KeyboardInterrupt:
        print()
        print("=" * 70)
        print("⚠ Import interrupted by user")
        print("=" * 70)
        return
    
    elapsed = time.time() - start_time
    
    # Summary
    print()
    print("=" * 70)
    print("Import Summary")
    print("=" * 70)
    print(f"Time elapsed:            {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"Unique ISBNs processed:  {successful + failed}")
    print(f"  - Successfully downloaded: {successful}")
    print(f"  - Not available:          {failed}")
    print(f"Total books updated:     {books_updated}")
    print(f"Remaining unique ISBNs:  {total - (successful + failed)}")
    print(f"Download rate:           {(successful + failed) / elapsed:.1f} covers/second")
    print()
    print("You can run this script again to continue downloading remaining covers.")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Download book covers from Open Library and store in database"
    )
    parser.add_argument(
        'database',
        nargs='?',
        default='library.db',
        help='Path to the library database (default: library.db)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of covers to download (for testing)'
    )
    
    args = parser.parse_args()
    
    # Resolve database path
    db_path = pathlib.Path(args.database)
    if not db_path.is_absolute():
        db_path = pathlib.Path(__file__).parent / db_path
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)
    
    # Run import
    import_covers(db_path, limit=args.limit)


if __name__ == '__main__':
    main()
