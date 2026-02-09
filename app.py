# ============================================================================
# IMPORTS
# ============================================================================

# Standard library imports
import asyncio
import atexit
import gzip
import hashlib
import io
import json
import os
import pathlib
import random
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from datetime import datetime, timedelta
from threading import Thread
from urllib.parse import quote, unquote

# Third-party imports
import pandas
import requests
from flask import Flask, render_template, request, redirect, jsonify, send_file, make_response

# ============================================================================
# APPLICATION SETUP
# ============================================================================

app = Flask(__name__)

# Database path configuration
db_path = pathlib.Path(__file__).parent / 'library.db'

# On-demand cover download rate limiting
cover_download_lock = threading.Lock()
last_cover_download_time = 0
COVER_RATE_LIMIT = 0.5  # seconds between downloads

# ============================================================================
# TEMPLATE FILTERS
# ============================================================================

# Allow regex search in templates
@app.template_filter('regex_search')
def regex_search_filter(s, pattern):
    if not s:
        return False
    return re.search(pattern, str(s)) is not None

# encode URL's for generated sitewide links
@app.template_filter('urlencode')
def urlencode_filter(s):
    return quote(str(s)) if s is not None else ''

# format due dates for display
@app.template_filter('due_date')
def due_date_filter(checkout_date_str):
    try:
        date_obj = datetime.strptime(checkout_date_str, '%Y-%m-%d')
        due = date_obj + timedelta(days=14)
        today = datetime.today().date()
        days_diff = (due.date() - today).days
        if days_diff > 0:
            return f"Due in {days_diff} day{'s' if days_diff != 1 else ''}"
        elif days_diff < 0:
            return f"Overdue by {abs(days_diff)} day{'s' if abs(days_diff) != 1 else ''}"
        else:
            return "Due today"
    except Exception:
        return ''

# format dates to be with month names nicely
@app.template_filter('nice_date')
def nice_date_filter(date_str):
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        day = date_obj.day
        if 4 <= day <= 20 or 24 <= day <= 30:
            suffix = "th"
        else:
            suffix = ["th", "st", "nd", "rd"][day % 10]
        
        return date_obj.strftime(f'%B {day}{suffix}, %Y')
    except Exception:
        return date_str

# color coding checkout hitstory
@app.template_filter('checkout_status')
def checkout_status_filter(checkout_date_str, return_date_str=None):
    """
    Returns the CSS color class for checkout status:
    - 'var(--bulma-primary)' if returned (green)
    - 'var(--bulma-danger)' if overdue (red)  
    - 'var(--bulma-warning)' if not returned but not overdue (yellow)
    """
    try:
        if return_date_str:
            return 'var(--bulma-primary)'
        
        checkout_date = datetime.strptime(checkout_date_str, '%Y-%m-%d')
        due_date = checkout_date + timedelta(days=14)
        today = datetime.today().date()
        
        if due_date.date() < today:
            return 'var(--bulma-danger)'
        else:
            return 'var(--bulma-warning)'
    except Exception:
        return 'var(--bulma-warning)' 

# Generate cover image URL or fallback to Open Library
@app.template_filter('cover_url')
def cover_url_filter(isbn, size='M'):
    """Generate cover image URL for a book by ISBN
    Returns Open Library cover URL as fallback if no local cover exists
    """
    if not isbn:
        return None
    # Clean ISBN
    clean = isbn.replace('-', '').replace(' ', '').strip()
    return f'https://covers.openlibrary.org/b/isbn/{clean}-{size}.jpg'

# ============================================================================
# BACKUP SYSTEM CONFIGURATION
# ============================================================================

# Backup configuration
BACKUP_DIRECTORY = pathlib.Path(__file__).parent / 'backups'
MAX_DAILY_BACKUPS = 7      # Keep 7 daily backups
MAX_FREQUENT_BACKUPS = 24  # Keep 24 hourly backups
MAX_EVENT_BACKUPS = 10     # Keep 10 event-based backups
BACKUP_ENABLED = True      # Global backup toggle

def ensure_backup_directory():
    """Ensure backup directory exists"""
    BACKUP_DIRECTORY.mkdir(exist_ok=True)
    (BACKUP_DIRECTORY / 'daily').mkdir(exist_ok=True)
    (BACKUP_DIRECTORY / 'frequent').mkdir(exist_ok=True)
    (BACKUP_DIRECTORY / 'events').mkdir(exist_ok=True)
    (BACKUP_DIRECTORY / 'manual').mkdir(exist_ok=True)

def get_db_path():

    return pathlib.Path(__file__).parent / 'library.db'

def create_backup(backup_type='manual', event_description=None):

    if not BACKUP_ENABLED:
        return False
        
    try:
        ensure_backup_directory()
        
        db_path = get_db_path()
        if not db_path.exists():
            print("Database file not found")
            return False
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        backup_subdir = BACKUP_DIRECTORY / backup_type
        backup_subdir.mkdir(exist_ok=True)
        
        if event_description:
            safe_desc = "".join(c for c in event_description if c.isalnum() or c in (' ', '_', '-')).rstrip()[:30]
            backup_filename = f'library_backup_{timestamp}_{safe_desc.replace(" ", "_")}.db.gz'
        else:
            backup_filename = f'library_backup_{timestamp}.db.gz'
            
        backup_path = backup_subdir / backup_filename
        
        with open(db_path, 'rb') as f_in:
            with gzip.open(backup_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'backup_type': backup_type,
            'event_description': event_description,
            'original_size': db_path.stat().st_size,
            'compressed_size': backup_path.stat().st_size,
            'compression_ratio': round(backup_path.stat().st_size / db_path.stat().st_size, 2),
            'version': '1.0'
        }
        
        metadata_path = backup_subdir / f'{backup_filename}.json'
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        cleanup_old_backups(backup_type)
        
        print(f"Backup created: {backup_path} ({backup_type})")
        return True
        
    except Exception as e:
        print(f"Backup failed: {e}")
        return False

def cleanup_old_backups(backup_type):
    """Remove old backup files based on type"""
    try:
        backup_subdir = BACKUP_DIRECTORY / backup_type
        if not backup_subdir.exists():
            return
            
        max_backups = {
            'daily': MAX_DAILY_BACKUPS,
            'frequent': MAX_FREQUENT_BACKUPS,
            'events': MAX_EVENT_BACKUPS,
            'manual': 5 
        }.get(backup_type, 5)
        
        backup_files = list(backup_subdir.glob('library_backup_*.db.gz'))
        backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        for old_backup in backup_files[max_backups:]:
            try:
                old_backup.unlink()
                metadata_file = backup_subdir / f'{old_backup.name}.json'
                if metadata_file.exists():
                    metadata_file.unlink()
                print(f"Removed old backup: {old_backup}")
            except Exception as e:
                print(f"Failed to remove {old_backup}: {e}")
                
    except Exception as e:
        print(f"Cleanup failed: {e}")

def restore_backup(backup_file):
    """Restore database from backup file"""
    try:
        backup_path = pathlib.Path(backup_file)
        if not backup_path.exists():
            return False, "Backup file not found"
            
        db_path = get_db_path()
        
        create_backup('events', f'pre_restore_backup')
        
        with gzip.open(backup_path, 'rb') as f_in:
            with open(db_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        print(f"Database restored from: {backup_path}")
        return True, "Database restored successfully"
        
    except Exception as e:
        print(f"Restore failed: {e}")
        return False, str(e)

def get_backup_list():
    """Get list of all available backups with metadata"""
    try:
        ensure_backup_directory()
        backups = []
        
        for backup_type in ['daily', 'frequent', 'events', 'manual']:
            backup_subdir = BACKUP_DIRECTORY / backup_type
            if not backup_subdir.exists():
                continue
                
            for backup_file in backup_subdir.glob('library_backup_*.db.gz'):
                metadata_file = backup_subdir / f'{backup_file.name}.json'
                
                metadata = {
                    'file_path': str(backup_file),
                    'backup_type': backup_type,
                    'timestamp': datetime.fromtimestamp(backup_file.stat().st_mtime).isoformat(),
                    'size': backup_file.stat().st_size
                }
                
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            stored_metadata = json.load(f)
                            metadata.update(stored_metadata)
                    except:
                        pass
                        
                backups.append(metadata)
        
        backups.sort(key=lambda x: x['timestamp'], reverse=True)
        return backups
        
    except Exception as e:
        print(f"Failed to get backup list: {e}")
        return []

def trigger_event_backup(event_description):
    """Trigger an event-based backup"""
    if BACKUP_ENABLED:
        Thread(target=lambda: create_backup('events', event_description)).start()

# ============================================================================
# CONTEXT PROCESSORS
# ============================================================================

@app.context_processor
def inject_classes():
    """Inject classes data into all templates"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM classes ORDER BY name ASC")
    classes = cur.fetchall()
    conn.close()
    return dict(classes=classes)

# ============================================================================
# API ROUTES - CRUD OPERATIONS
# ============================================================================
@app.route('/add_book', methods=['POST'])
def add_book():
    data = request.get_json()
    required_fields = ['title', 'author', 'localnumber']
    for field in required_fields:
        if not data.get(field):
            return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO books (title, subtitle, author, publisher, published, isbn, localnumber, call1, call2, booklocation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('title'),
            data.get('subtitle'),
            data.get('author'),
            data.get('publisher'),
            data.get('published_date'),
            data.get('isbn'),
            data.get('localnumber'),
            data.get('call1'),
            data.get('call2'),
            data.get('location')
        ))
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True})


@app.route('/remove_books', methods=['POST'])
def remove_books():

    data = request.get_json()
    book_ids = data.get('book_ids', [])
    
    if not book_ids:
        return jsonify({'success': False, 'error': 'No books selected'}), 400
    
    try:

        trigger_event_backup('bulk_book_deletion')
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        

        placeholders = ','.join('?' * len(book_ids))
        cur.execute(f'DELETE FROM books WHERE rowid IN ({placeholders})', book_ids)
        deleted_count = cur.rowcount
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully removed {deleted_count} book(s)'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/update_book/<int:book_id>', methods=['POST'])
def update_book(book_id):
    """Update book information"""
    data = request.get_json()
    
    required_fields = ['title', 'author', 'localnumber']
    for field in required_fields:
        if not data.get(field):
            return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
    
    try:
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Update book information
        cur.execute('''
            UPDATE books SET 
                title = ?,
                subtitle = ?,
                author = ?,
                publisher = ?,
                published = ?,
                isbn = ?,
                localnumber = ?,
                call1 = ?,
                call2 = ?,
                booklocation = ?
            WHERE rowid = ?
        ''', (
            data.get('title'),
            data.get('subtitle'),
            data.get('author'),
            data.get('publisher'),
            data.get('published'),
            data.get('isbn'),
            data.get('localnumber'),
            data.get('call1'),
            data.get('call2'),
            data.get('booklocation'),
            book_id
        ))
        
        if cur.rowcount == 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Book not found'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Book updated successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# Open Library API lookup for ISBN autofill
@app.route('/lookup_isbn', methods=['POST'])
def lookup_isbn():
    """Lookup book information by ISBN using Open Library API"""
    print('DEBUG: /lookup_isbn endpoint called')
    data = request.get_json()
    isbn = data.get('isbn', '').replace('-', '').strip()
    if not isbn:
        return {'success': False, 'error': 'No ISBN provided'}, 400
    url = f'https://openlibrary.org/isbn/{isbn}.json'
    try:
        resp = requests.get(url, timeout=5)
        print(resp)
        if resp.status_code == 404:
            return {'success': False, 'error': 'No book found for this ISBN'}, 404
        resp.raise_for_status()
        print('DEBUG: Open Library JSON:', resp.json())
        book_data = resp.json()
        result = {}
        if 'title' in book_data:
            result['title'] = book_data['title']
        if 'publishers' in book_data:
            result['publisher'] = book_data['publishers']
        if 'publish_date' in book_data:
            result['published_date'] = book_data['publish_date']
        if 'authors' in book_data and book_data['authors']:
            author_keys = [a['key'] for a in book_data['authors'] if 'key' in a]
            authors = []
            for key in author_keys:
                author_url = f'https://openlibrary.org{key}.json'
                try:
                    author_resp = requests.get(author_url, timeout=3)
                    if author_resp.status_code == 200:
                        author_info = author_resp.json()
                        if 'name' in author_info:
                            authors.append(author_info['name'])
                except Exception:
                    continue
            if authors:
                result['author'] = ', '.join(authors)
        return {'success': True, 'data': result}
    except Exception as e:
        return {'success': False, 'error': str(e)}, 500

# Add checkout via modal form
@app.route('/add_checkout', methods=['POST'])
def add_checkout():
    """Add a new book checkout record"""
    print('DEBUG: /add_checkout endpoint called')
    data = request.get_json()
    print('DEBUG: Checkout data received:', data)
    required_fields = ['student_id', 'book_title', 'checkout_date']
    for field in required_fields:
        if not data.get(field):
            return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
    
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        # Get book rowid by title
        cur.execute("SELECT rowid FROM books WHERE title = ?", (data.get('book_title'),))
        book = cur.fetchone()
        if not book:
            conn.close()
            return jsonify({'success': False, 'error': 'Book not found'}), 404
        
        print(f'DEBUG: Inserting checkout - student_id: {data.get("student_id")}, book_id: {book[0]}, date: {data.get("checkout_date")}')
        cur.execute('''
            INSERT INTO checkouts (student_id, book_id, checkout_date)
            VALUES (?, ?, ?)
        ''', (
            data.get('student_id'),
            book[0],
            data.get('checkout_date')
        ))
        conn.commit()
        print('DEBUG: Checkout inserted successfully')
    except Exception as e:
        print('DEBUG: Checkout insert failed:', str(e))
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True})

# Search students for input field autocomplete
@app.route('/search_students')
def search_students():
    """Search students for autocomplete functionality"""
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('''
        SELECT id, name, fax_id, class_id FROM students 
        WHERE name LIKE ? OR fax_id LIKE ?
        LIMIT 10
    ''', (f'%{q}%', f'%{q}%'))
    results = cur.fetchall()
    conn.close()
    return jsonify([dict(row) for row in results])

# Search books for input autocomplete
@app.route('/search_books')
def search_books():
    """Search books for autocomplete functionality"""
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'books': []})
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('''
        SELECT rowid as id, title, author, localnumber FROM books 
        WHERE title LIKE ? OR author LIKE ? OR localnumber LIKE ?
        LIMIT 10
    ''', (f'%{q}%', f'%{q}%', f'%{q}%'))
    results = cur.fetchall()
    conn.close()
    return jsonify({'books': [dict(row) for row in results]})

# Get next available localnumber
@app.route('/next_localnumber')
def next_localnumber():
    """Get the next available localnumber (max + 1)"""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute('SELECT localnumber FROM books')
        all_localnumbers = cur.fetchall()
        
        numeric_localnumbers = []
        for (localnumber,) in all_localnumbers:
            if localnumber and str(localnumber).isdigit():
                numeric_localnumbers.append(int(localnumber))
        
        next_number = max(numeric_localnumbers) + 1 if numeric_localnumbers else 1
        conn.close()
        return jsonify({'next_localnumber': str(next_number)})
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

# ============================================================================
# CLASS MANAGEMENT ROUTES
# ============================================================================

# Route for creating a new class
@app.route('/add_class', methods=['POST'])
def add_class():
    """Create a new class and assign students to it"""
    try:
        trigger_event_backup('class_creation')
        
        data = request.get_json()
        class_name = data.get('class_name', '').strip()
        teacher_name = data.get('teacher_name', '').strip()
        student_ids = data.get('student_ids', [])
        
        if not class_name:
            return jsonify({'success': False, 'error': 'Class name is required'})
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute('INSERT INTO classes (name, teacher_name) VALUES (?, ?)', 
                    (class_name, teacher_name if teacher_name else None))
        class_id = cur.lastrowid
        
        if student_ids:
            placeholders = ','.join('?' * len(student_ids))
            cur.execute(f'UPDATE students SET class_id = ? WHERE id IN ({placeholders})', 
                        [class_id] + student_ids)
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Class "{class_name}" created successfully with {len(student_ids)} students'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# Site wide route for class data
@app.route('/get_class/<int:class_id>')
def get_class(class_id):
    """Get class information and associated students"""
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute('SELECT id, name, teacher_name FROM classes WHERE id = ?', (class_id,))
        class_data = cur.fetchone()
        
        if not class_data:
            return jsonify({'success': False, 'error': 'Class not found'})
        
        cur.execute('SELECT id, name, fax_id FROM students WHERE class_id = ?', (class_id,))
        students = cur.fetchall()
        
        conn.close()
        
        return jsonify({
            'success': True,
            'class': dict(class_data),
            'students': [dict(student) for student in students]
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# Route for changing class data
@app.route('/update_class/<int:class_id>', methods=['POST'])
def update_class(class_id):
    """Update class information and student assignments"""
    try:
        trigger_event_backup(f'class_update_{class_id}')
        
        data = request.get_json()
        class_name = data.get('class_name', '').strip()
        teacher_name = data.get('teacher_name', '').strip()
        student_ids = data.get('student_ids', [])
        
        if not class_name:
            return jsonify({'success': False, 'error': 'Class name is required'})
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute('UPDATE classes SET name = ?, teacher_name = ? WHERE id = ?', 
                    (class_name, teacher_name if teacher_name else None, class_id))
        
        cur.execute('UPDATE students SET class_id = NULL WHERE class_id = ?', (class_id,))
        
        if student_ids:
            placeholders = ','.join('?' * len(student_ids))
            cur.execute(f'UPDATE students SET class_id = ? WHERE id IN ({placeholders})', 
                        [class_id] + student_ids)
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Class "{class_name}" updated successfully with {len(student_ids)} students'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/delete_class/<int:class_id>', methods=['DELETE'])
def delete_class(class_id):
    """Delete a class and unassign its students"""
    try:
        trigger_event_backup(f'class_deletion_{class_id}')
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute('SELECT name FROM classes WHERE id = ?', (class_id,))
        result = cur.fetchone()
        if not result:
            conn.close()
            return jsonify({'success': False, 'error': 'Class not found'})
        
        class_name = result[0]
        
        cur.execute('UPDATE students SET class_id = NULL WHERE class_id = ?', (class_id,))
        
        cur.execute('DELETE FROM classes WHERE id = ?', (class_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Class "{class_name}" deleted successfully. Students have been unassigned.'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ============================================================================
# STUDENT MANAGEMENT ROUTES
# ============================================================================

# Add student via modal form
@app.route('/add_student', methods=['POST'])
def add_student():
    """Add a new student with auto-generated FAX ID"""
    data = request.get_json()
    required_fields = ['name']
    for field in required_fields:
        if not data.get(field):
            return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
    
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:

        cur.execute('SELECT fax_id FROM students')
        all_fax_ids = cur.fetchall()
        
        numeric_fax_ids = []
        for (fax_id,) in all_fax_ids:
            if fax_id and fax_id.isdigit():
                numeric_fax_ids.append(int(fax_id))
        
        next_fax_id = max(numeric_fax_ids) + 1 if numeric_fax_ids else 1000
        
        class_id_value = data.get('class_id')
        if class_id_value and class_id_value.strip():
            try:

                class_id = int(class_id_value)
            except ValueError:

                class_id = None
        else:
            class_id = None
        
        cur.execute('''
            INSERT INTO students (name, fax_id, class_id)
            VALUES (?, ?, ?)
        ''', (
            data.get('name'),
            str(next_fax_id),
            class_id
        ))
        conn.commit()
        

        result_data = {'success': True, 'fax_id': str(next_fax_id)}
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
    conn.close()
    return jsonify(result_data)

# ============================================================================
# PAGE ROUTES
# ============================================================================

# Search page
@app.route("/search_page")
def class_search():
    """Render the search page"""
    return render_template('search.html', color='blue')

# Settings page
@app.route("/settings")
def settings():
    """Render the settings page with class management data"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get all classes for the manage classes modal
    cur.execute('SELECT id, name, teacher_name FROM classes ORDER BY name')
    classes = cur.fetchall()
    
    conn.close()
    
    return render_template('settings.html', classes=[dict(row) for row in classes])

# ============================================================================
# AUTHENTICATION SYSTEM
# ============================================================================

def verify_password(stored_password, provided_password):
    """Verify password using PBKDF2 hashing"""
    salt_hex, key_hex = stored_password.split(':')
    salt = bytes.fromhex(salt_hex)
    new_key = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)
    return secrets.compare_digest(new_key.hex(), key_hex)

@app.before_request
def check_authentication():
    """Check authentication for all routes except login and static files"""
    # Allow access to login page and static files
    if request.endpoint in ['login'] or request.path.startswith('/static/'):
        return
    
    # Check for persistent token cookie
    token = request.cookies.get('cookie')
    if not token:
        return redirect('/login')
    
    # Verify token against database
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        cursor.execute('''
            SELECT u.username FROM uauth u
            JOIN uauth_cookies c ON u.id = c.user_id
            WHERE c.cookie = ?
        ''', (token_hash,))
        result = cursor.fetchone()
        conn.close()
        if not result:
            # Invalid token, redirect to login
            return redirect('/login')
    except Exception:
        # Database error, redirect to login for safety
        return redirect('/login')

# Login page
@app.route("/login", methods=['GET', 'POST'])
def login():
    """Handle user login with persistent tokens"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM sqlite_master WHERE type="table" AND name="uauth"')
    has_uauth = cursor.fetchone()[0]
    if not has_uauth:
        # Table doesn't exist, create it
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS uauth (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS uauth_cookies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                cookie TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES uauth(id)
            )
        """)
        conn.commit()
    cursor.execute('SELECT COUNT(*) FROM uauth')
    user_count = cursor.fetchone()[0]
    if user_count == 0:
        # No user exists, show signup page
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            if username and password:
                # Hash password and create user
                salt = secrets.token_bytes(16)
                key = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)
                password_hash = f"{salt.hex()}:{key.hex()}"
                try:
                    cursor.execute('INSERT INTO uauth (username, password) VALUES (?, ?)', (username, password_hash))
                    conn.commit()
                    # Log user in immediately
                    user_id = cursor.lastrowid
                    raw_token = secrets.token_urlsafe(32)
                    hash = hashlib.sha256(raw_token.encode()).hexdigest()
                    cursor.execute('INSERT INTO uauth_cookies (user_id, cookie) VALUES (?, ?)', (user_id, hash))
                    conn.commit()
                    conn.close()
                    response = make_response(redirect('/'))
                    response.set_cookie('cookie', value=raw_token, max_age=315360000, httponly=True, samesite='Lax')
                    return response
                except sqlite3.IntegrityError:
                    return render_template('auth/login.html', error="Username already exists", signup=True)
            else:
                return render_template('auth/login.html', error="Please enter both username and password", signup=True)
        conn.close()
        return render_template('auth/login.html', signup=True)
    else:
        # User exists, show login page
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            print(f"Login attempt - Username: {username}, Password: {password}")
            if username and password:
                cursor.execute('SELECT id, password FROM uauth WHERE username = ?', (username,))
                user_row = cursor.fetchone()
                if user_row and verify_password(user_row[1], password):
                    user_id = user_row[0]
                    raw_token = secrets.token_urlsafe(32)
                    hash = hashlib.sha256(raw_token.encode()).hexdigest()
                    cursor.execute('INSERT INTO uauth_cookies (user_id, cookie) VALUES (?, ?)', (user_id, hash))
                    conn.commit()
                    conn.close()
                    response = make_response(redirect('/'))
                    response.set_cookie('cookie', value=raw_token, max_age=315360000, httponly=True, samesite='Lax')
                    return response
                else:
                    conn.close()
                    return render_template('auth/login.html', error="Invalid username or password")
            else:
                conn.close()
                return render_template('auth/login.html', error="Please enter both username and password")
        conn.close()
        return render_template('auth/login.html')

# Logout route
@app.route("/logout", methods=['POST'])
def logout():
    """Handle user logout and clear persistent tokens"""
    # Clear the persistent token from database
    token = request.cookies.get('cookie')
    if token:
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            cursor.execute('DELETE FROM uauth_cookies WHERE cookie = ?', (token_hash,))
            conn.commit()
            conn.close()
        except Exception:
            pass  # If database error, still clear the cookie
    
    response = make_response(redirect('/login'))
    response.set_cookie('cookie', '', expires=0)
    return response

# Welcome/Landing page
@app.route("/welcome")
def welcome_page():
    """Render the welcome/landing page with library overview"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get statistics for the welcome page
    cur.execute('SELECT COUNT(*) as total_books FROM books')
    book_count = cur.fetchone()['total_books']
    
    cur.execute('SELECT COUNT(*) as total_students FROM students')
    student_count = cur.fetchone()['total_students']
    
    cur.execute('SELECT COUNT(*) as total_classes FROM classes')
    class_count = cur.fetchone()['total_classes']
    
    cur.execute('SELECT COUNT(*) as outstanding FROM checkouts WHERE return_date IS NULL')
    outstanding_count = cur.fetchone()['outstanding']
    
    conn.close()
    
    return render_template('welcome.html', 
                         book_count=book_count,
                         student_count=student_count,
                         class_count=class_count,
                         outstanding_count=outstanding_count)

# Main page load
@app.route("/")
def main_page():
    """Render the main dashboard with outstanding book checkouts"""
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get outstanding books (not yet returned)
    cur.execute('''
        SELECT b.title, b.localnumber, s.name as student_name, s.fax_id as student_fax_id, c.checkout_date, c.id as checkout_id
        FROM checkouts c
        JOIN books b ON c.book_id = b.rowid
        JOIN students s ON c.student_id = s.id
        WHERE c.return_date IS NULL
        ORDER BY c.checkout_date ASC, c.id ASC
    ''')
    outstanding_books = cur.fetchall()
    conn.close()
    
    return render_template('main.html', outstanding_books=outstanding_books)


# ============================================================================
# FILE UPLOAD AND EXPORT ROUTES
# ============================================================================

# File upload routes for settings
@app.route('/upload_books', methods=['POST'])
def upload_books():
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file selected'})
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    if not file.filename.lower().endswith('.csv'):
        return jsonify({'success': False, 'error': 'Please select a CSV file'})

    # Trigger backup before major data change
    trigger_event_backup('bulk_book_import')

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            file.save(temp_file.name)
            temp_path = temp_file.name

        print('[upload_books] Reading CSV into DataFrame...')
        df = pandas.read_csv(temp_path, quotechar='"', doublequote=True, engine='python', on_bad_lines='skip', dtype=str)
        print(f'[upload_books] DataFrame shape: {df.shape}')
        print(f'[upload_books] DataFrame columns: {list(df.columns)}')

        expected_cols = [
            'Local Number', 'Title', 'Sub Title', 'Author(s)', 
            'Call 1', 'Call 2', 'Publisher', 'Published', 'ISBN #', 'Location'
        ]
        present_cols = [col for col in expected_cols if col in df.columns]
        print(f'[upload_books] Present columns: {present_cols}')
        df_selected = df[present_cols].copy()
        col_map = {
            'Local Number': 'localnumber',
            'Title': 'title',
            'Sub Title': 'subtitle',
            'Author(s)': 'author',
            'Call 1': 'call1',
            'Call 2': 'call2',
            'Publisher': 'publisher',
            'Published': 'published',
            'ISBN #': 'isbn',
            'Location': 'booklocation'
        }
        df_selected.rename(columns=col_map, inplace=True)
        
        # Add cover_image column as NULL so it's preserved during import
        df_selected['cover_image'] = None

        db_path = pathlib.Path(__file__).parent / 'library.db'
        conn = sqlite3.connect(str(db_path))
        df_selected.to_sql('books', conn, index=False, if_exists='replace')
        conn.close()
        print('[upload_books] Database write complete.')
        import os
        os.unlink(temp_path)
        return jsonify({'success': True, 'message': 'Book list imported successfully'})
    except Exception as e:
        import traceback
        print('[upload_books] Exception occurred:')
        traceback.print_exc()
        return jsonify({'success': False, 'error': f'Import failed: {str(e)}'})

# Convert book data to CSV
@app.route('/export_books')
def export_books():
    """Export all books to CSV format matching import template"""
    import io
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    # Use the same columns/order as import_books.py expects
    columns = [
        'Local Number', 'Title', 'Sub Title', 'Author(s)',
        'Call 1', 'Call 2', 'Publisher', 'Published', 'ISBN #', 'Location'
    ]
    db_cols = [
        'localnumber', 'title', 'subtitle', 'author',
        'call1', 'call2', 'publisher', 'published', 'isbn', 'booklocation'
    ]
    cur.execute(f'SELECT {", ".join(db_cols)} FROM books ORDER BY title')
    books = cur.fetchall()
    conn.close()
    output = io.StringIO()
    # Build DataFrame from books
    data = []
    for book in books:
        data.append({
            'Local Number': book['localnumber'] or '',
            'Title': book['title'] or '',
            'Sub Title': book['subtitle'] or '',
            'Author(s)': book['author'] or '',
            'Call 1': book['call1'] or '',
            'Call 2': book['call2'] or '',
            'Publisher': book['publisher'] or '',
            'Published': book['published'] or '',
            'ISBN #': book['isbn'] or '',
            'Location': book['booklocation'] or ''
        })
    df = pandas.DataFrame(data, columns=columns)
    df.to_csv(output, index=False, quoting=1)  # quoting=1 is csv.QUOTE_ALL
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=books_export.csv'
    response.headers['Content-type'] = 'text/csv'
    return response

# Find unreturned books and export to CSV
@app.route('/export_outstanding_students')
def export_outstanding_students():
    """Export students with outstanding book checkouts to CSV"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute('''
        SELECT s.name as student_name, b.title as book_title, c.checkout_date
        FROM students s
        JOIN checkouts c ON s.id = c.student_id
        JOIN books b ON c.book_id = b.rowid
        WHERE c.return_date IS NULL
        ORDER BY s.name, c.checkout_date
    ''')
    checkouts = cur.fetchall()
    conn.close()
    
    output = io.StringIO()
    output.write('student_name,book_title,checkout_date\n')
    
    for checkout in checkouts:
        output.write(f'"{checkout["student_name"]}","{checkout["book_title"]}","{checkout["checkout_date"]}"\n')
    
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = 'attachment; filename=outstanding_students.csv'
    response.headers['Content-type'] = 'text/csv'
    
    return response

@app.route('/upload_students', methods=['POST'])
def upload_students():
    """Handle bulk student import via CSV upload"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file selected'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    
    if file and file.filename.lower().endswith('.csv'):
        try:
            # Trigger backup before major data change
            trigger_event_backup('bulk_student_import')
            
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as temp_file:
                file.save(temp_file.name)
                temp_csv_path = temp_file.name
            
            script_path = os.path.join(os.path.dirname(__file__), 'import_students.py')
            db_path = os.path.join(os.path.dirname(__file__), 'library.db')
            
            # Call the import_students.py script
            result = subprocess.run(
                [sys.executable, script_path, temp_csv_path, db_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            os.unlink(temp_csv_path)
            
            if result.returncode == 0:

                output_lines = result.stdout.strip().split('\n')
                success_line = next((line for line in output_lines if line.startswith('SUCCESS:')), '')
                message = success_line.replace('SUCCESS: ', '') if success_line else 'Students imported successfully'
                
                warnings = []
                in_warnings = False
                for line in output_lines:
                    if line == 'WARNINGS:':
                        in_warnings = True
                        continue
                    elif in_warnings and line.startswith('  - '):
                        warnings.append(line[4:])
                
                response = {'success': True, 'message': message}
                if warnings:
                    response['warnings'] = warnings
                    
                return jsonify(response)
            else:
                error_lines = result.stdout.strip().split('\n')
                error_line = next((line for line in error_lines if line.startswith('ERROR:')), '')
                error_message = error_line.replace('ERROR: ', '') if error_line else 'Unknown error occurred'
                return jsonify({'success': False, 'error': error_message})
                
        except subprocess.TimeoutExpired:
            return jsonify({'success': False, 'error': 'Import operation timed out'})
        except Exception as e:
            try:
                if 'temp_csv_path' in locals():
                    os.unlink(temp_csv_path)
            except:
                pass
            return jsonify({'success': False, 'error': f'Error processing file: {str(e)}'})
    else:
        return jsonify({'success': False, 'error': 'Please select a CSV file'})

@app.route('/upload_backup', methods=['POST'])
def upload_backup():
    """Handle database backup file upload"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file selected'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    
    if file and (file.filename.lower().endswith('.db') or file.filename.lower().endswith('.sqlite')):

        return jsonify({'success': True, 'message': 'Backup loaded successfully'})
    else:
        return jsonify({'success': False, 'error': 'Please select a database file (.db or .sqlite)'})


# ============================================================================
# SEARCH FUNCTIONALITY
# ============================================================================

@app.route('/search')
def search():
    """Full-text search across books and students with HTML results"""
    q = request.args.get('q', '').strip()
    results = []
    if q:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        try:

            safe_q = re.sub(r'["*#^\[\]{}()]', '', q)
            if safe_q:  
                cur.execute('''
                    SELECT b.title, b.author, b.localnumber, b.booklocation
                    FROM books_fts f
                    JOIN books b ON b.rowid = f.rowid
                    WHERE books_fts MATCH ?
                    LIMIT 30
                ''', (safe_q,))
                results = cur.fetchall()
        except Exception:
            results = []
        
        if not results and q:
            cur.execute('''
                SELECT title, author, localnumber, booklocation
                FROM books
                WHERE title LIKE ? OR author LIKE ?
                LIMIT 30
            ''', (f'%{q}%', f'%{q}%'))
            results = cur.fetchall()
            
        conn.close()

    student_results = []
    if q:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # get search results
        cur.execute('''
            SELECT s.id, s.name, s.fax_id, c.name as class_name, c.teacher_name 
            FROM students s
            LEFT JOIN classes c ON s.class_id = c.id
            WHERE s.name LIKE ? OR s.fax_id LIKE ?
        ''', (f'%{q}%', f'%{q}%'))
        student_results = list(cur.fetchall())
        

        cur.execute('''
            SELECT s.id, s.name, s.fax_id, c.name as class_name, c.teacher_name 
            FROM students s
            JOIN classes c ON s.class_id = c.id
            WHERE c.name LIKE ? OR c.teacher_name LIKE ?
        ''', (f'%{q}%', f'%{q}%'))
        class_student_results = cur.fetchall()
        

        existing_ids = {s['id'] for s in student_results}
        for student in class_student_results:
            if student['id'] not in existing_ids:
                student_results.append(student)
        
        conn.close()
    # display search results
    html = '<div style="display: flex; flex-direction: column; gap: 0.75rem;">'
    
    # Book results with covers
    for row in results:
        book_url = '/book/' + urllib.parse.quote(str(row["localnumber"]))
        cover_url = f'/api/book/{urllib.parse.quote(str(row["localnumber"]))}/cover'
        
        html += f'''
        <a href="{book_url}" style="text-decoration: none;">
            <div style="border-radius: 0.75rem; overflow: hidden; background: linear-gradient(90deg, #14161a 0%, #2b2d31 100%); display: flex; height: 140px; transition: transform 0.2s;" onmouseover="this.style.transform='scale(1.02)'" onmouseout="this.style.transform='scale(1)'">
                <div style="width: 100px; min-width: 100px; background: #1a1c20; display: flex; align-items: center; justify-content: center; padding: 0.5rem;">
                    <img src="{cover_url}" alt="Book cover" style="max-width: 100%; max-height: 100%; object-fit: contain; border-radius: 4px; box-shadow: 0 2px 8px rgba(0,0,0,0.3);" onerror="this.style.display='none'; this.parentElement.innerHTML='<div style=\\'text-align: center; color: #6a9a75;\\'><i class=\\'fas fa-book fa-2x\\'></i></div>'">
                </div>
                <div style="flex: 1; padding: 1rem; display: flex; flex-direction: column; justify-content: center; min-width: 0;">
                    <div class="has-text-primary" style="font-weight: 600; font-size: 1.1rem; margin-bottom: 0.25rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{row["title"]}</div>
                    <div style="color: white; margin-bottom: 0.25rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{row["author"]}</div>
                    <div style="color: gray; font-size: 0.9rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">Location: {row["booklocation"]} • #{row["localnumber"]}</div>
                </div>
            </div>
        </a>'''
    
    # Student results
    for student in student_results:
        student_url = f'/student/{student["fax_id"]}'
        html += f'''
        <a href="{student_url}" style="text-decoration: none;">
            <div style="border-radius: 0.75rem; overflow: hidden; background: linear-gradient(90deg, #14161a 0%, #2b2d31 100%); display: flex; align-items: center; padding: 1rem; min-height: 60px; transition: transform 0.2s;" onmouseover="this.style.transform='scale(1.02)'" onmouseout="this.style.transform='scale(1)'">
                <div style="margin-right: 1rem; color: #5bc0de;">
                    <i class="fas fa-user fa-2x"></i>
                </div>
                <div style="flex: 1; min-width: 0;">
                    <div class="has-text-info" style="font-weight: 600; font-size: 1.1rem; margin-bottom: 0.25rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{student["name"]}</div>
                    <div style="color: white; font-size: 0.9rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{student["teacher_name"]} • {student["class_name"]}</div>
                </div>
            </div>
        </a>'''
    
    html += '</div>'
    if not results and not student_results and q:
        html = f'<p>No results for "{q}"</p>'
    elif not q:
        html = ''
    return html

# ============================================================================
# DETAIL PAGE ROUTES
# ============================================================================

# Book detail page
@app.route('/book/<localnumber>')
def book_detail(localnumber):
    """Display detailed information for a specific book"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT rowid, * FROM books WHERE localnumber = ?", (localnumber,))
    book = cur.fetchone()
    checkouts = []
    if book:
        cur.execute('''
            SELECT s.fax_id as student_fax_id, s.name as student_name, c.checkout_date, c.return_date, c.id as checkout_id
            FROM checkouts c
            JOIN students s ON c.student_id = s.id
            WHERE c.book_id = ?
            ORDER BY c.checkout_date DESC, c.id DESC
            LIMIT 20
        ''', (book['rowid'],))
        checkouts = cur.fetchall()
    conn.close()
    if not book:
        return f"No book found with localnumber: {localnumber}", 404
    return render_template('book_detail.html', book=book, checkouts=checkouts)

# Student detail page
@app.route('/student/<student_id>')
def student_detail(student_id):
    """Display detailed information for a specific student"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('''
        SELECT s.*, c.name as class_name, c.teacher_name as teacher_name
        FROM students s
        LEFT JOIN classes c ON s.class_id = c.id
        WHERE s.fax_id = ?
    ''', (student_id,))
    student = cur.fetchone()
    checkouts = []
    if student:
        cur.execute('''
            SELECT b.title, b.localnumber, b.author, c.checkout_date, c.return_date, c.id as checkout_id
            FROM checkouts c
            JOIN books b ON c.book_id = b.rowid
            WHERE c.student_id = ?
            ORDER BY c.checkout_date DESC, c.id DESC
            LIMIT 20
        ''', (student['id'],))
        checkouts = cur.fetchall()
    conn.close()
    if not student:
        return f"No student found with id: {student_id}", 404
    return render_template('student_detail.html', student=student, checkouts=checkouts)

# Class detail page
@app.route('/class/<class_name>')
def class_detail(class_name):
    """Display detailed information for a specific class"""
    class_name = unquote(class_name)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('SELECT * FROM classes WHERE name = ?', (class_name,))
    class_info = cur.fetchone()
    students = []
    if class_info:
        cur.execute('''
            SELECT s.id, s.name, s.fax_id
            FROM students s
            WHERE s.class_id = ?
            ORDER BY s.name ASC
        ''', (class_info['id'],))
        students = cur.fetchall()
    conn.close()
    if not class_info:
        return f"No class found with name: {class_name}", 404
    return render_template('class_detail.html', class_info=class_info, students=students)

# ============================================================================
# API ENDPOINTS
# ============================================================================

# API endpoint to get book details by local number
@app.route('/api/book/<localnumber>')
def book_api(localnumber):
    """JSON API endpoint for book information including checkout status"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT rowid, * FROM books WHERE localnumber = ?", (localnumber,))
    book = cur.fetchone()
    if not book:
        conn.close()
        return {'error': 'Book not found'}, 404
    
    # Check if book is currently checked out
    cur.execute('''
        SELECT s.name as student_name, s.fax_id as student_fax_id, c.checkout_date
        FROM checkouts c
        JOIN students s ON c.student_id = s.id
        WHERE c.book_id = ? AND c.return_date IS NULL
    ''', (book['rowid'],))
    active_checkout = cur.fetchone()
    
    conn.close()
    
    result = {
        'title': book['title'],
        'subtitle': book['subtitle'],
        'author': book['author'],
        'localnumber': book['localnumber']
    }
    
    if active_checkout:
        result['checked_out'] = True
        result['checked_out_to'] = active_checkout['student_name']
        result['checked_out_to_fax_id'] = active_checkout['student_fax_id']
        result['checkout_date'] = active_checkout['checkout_date']
    else:
        result['checked_out'] = False
    
    return result


# API endpoint to get book cover image
@app.route('/api/book/<localnumber>/cover')
def book_cover(localnumber):
    """Serve book cover image from database, with on-demand downloading (rate-limited)"""
    global last_cover_download_time
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT cover_image, isbn FROM books WHERE localnumber = ?", (localnumber,))
    book = cur.fetchone()
    
    if not book:
        conn.close()
        return {'error': 'Book not found'}, 404
    
    # If we have a cover image in the database, serve it
    # Skip if it's the 'NO_COVER' marker
    if book['cover_image'] and book['cover_image'] != b'NO_COVER':
        conn.close()
        response = make_response(book['cover_image'])
        response.headers['Content-Type'] = 'image/jpeg'
        response.headers['Cache-Control'] = 'public, max-age=31536000'  # Cache for 1 year
        return response
    
    # If already marked as NO_COVER, don't retry
    if book['cover_image'] == b'NO_COVER':
        conn.close()
        return {'error': 'No cover available'}, 404
    
    # Try to download the cover on-demand (with rate limiting)
    isbn = book['isbn']
    if isbn:
        # Try to acquire the lock without blocking
        if cover_download_lock.acquire(blocking=False):
            try:
                # Check if enough time has passed since last download
                current_time = time.time()
                time_since_last = current_time - last_cover_download_time
                
                if time_since_last >= COVER_RATE_LIMIT:
                    # Enough time has passed, download the cover
                    clean_isbn = isbn.replace('-', '').replace(' ', '').strip()
                    url = f'https://covers.openlibrary.org/b/isbn/{clean_isbn}-L.jpg'
                    
                    try:
                        response = requests.get(url, timeout=5)
                        # Accept images >= 500 bytes (more lenient than 1000)
                        if response.status_code == 200 and len(response.content) >= 500:
                            # Valid cover image, save it
                            cur.execute(
                                "UPDATE books SET cover_image = ? WHERE localnumber = ?",
                                (response.content, localnumber)
                            )
                            conn.commit()
                            last_cover_download_time = time.time()
                            conn.close()
                            
                            # Serve the freshly downloaded cover
                            img_response = make_response(response.content)
                            img_response.headers['Content-Type'] = 'image/jpeg'
                            img_response.headers['Cache-Control'] = 'public, max-age=31536000'
                            return img_response
                        elif response.status_code == 404:
                            # API explicitly says cover not found, mark it to avoid retries
                            cur.execute(
                                "UPDATE books SET cover_image = ? WHERE localnumber = ?",
                                (b'NO_COVER', localnumber)
                            )
                            conn.commit()
                            last_cover_download_time = time.time()
                            conn.close()
                            return {'error': 'No cover available'}, 404
                        else:
                            # API returned non-200 (not 404), might be temporary issue
                            # Don't mark as NO_COVER, don't update rate limit timer
                            conn.close()
                            return {'error': 'Cover service temporarily unavailable'}, 503
                    except Exception as e:
                        # Network error or timeout, don't mark as NO_COVER (might be temporary)
                        # Don't update rate limit timer to allow quicker retry
                        conn.close()
                        return {'error': 'Cover service temporarily unavailable'}, 503
                else:
                    # Too soon since last download, return 503 for now
                    conn.close()
                    return {'error': 'Rate limited - try again later'}, 503
            finally:
                cover_download_lock.release()
        else:
            # Another download is in progress, return 503 for now
            conn.close()
            return {'error': 'Download in progress'}, 503
    
    # No ISBN, can't download
    conn.close()
    return {'error': 'No cover available'}, 404


# API endpoint to upload a book cover image
@app.route('/api/book/<localnumber>/cover/upload', methods=['POST'])
def upload_book_cover(localnumber):
    """Upload a custom cover image for a book"""
    if 'cover' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'}), 400
    
    file = request.files['cover']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    # Validate it's an image file
    allowed_extensions = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
    if '.' not in file.filename or file.filename.rsplit('.', 1)[1].lower() not in allowed_extensions:
        return jsonify({'success': False, 'error': 'Only image files are allowed'}), 400
    
    try:
        # Read and store the image in the database
        image_data = file.read()
        
        # Validate it's a real image and not too large
        if len(image_data) > 5 * 1024 * 1024:  # 5MB limit
            return jsonify({'success': False, 'error': 'File too large (max 5MB)'}), 400
        
        if len(image_data) < 100:  # Minimum reasonable size
            return jsonify({'success': False, 'error': 'File too small'}), 400
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Check if book exists
        cur.execute("SELECT rowid FROM books WHERE localnumber = ?", (localnumber,))
        book = cur.fetchone()
        if not book:
            conn.close()
            return jsonify({'success': False, 'error': 'Book not found'}), 404
        
        # Update the cover_image column
        cur.execute("UPDATE books SET cover_image = ? WHERE localnumber = ?", (image_data, localnumber))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Cover uploaded successfully'}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# Book return endpoint
@app.route('/return_book', methods=['POST'])
def return_book():
    """Process book returns with background database update"""
    checkout_id = request.form.get('checkout_id')
    redirect_url = request.form.get('redirect_url', '/')
    
    def update_database():
        conn = sqlite3.connect(pathlib.Path(__file__).parent / 'library.db')
        cur = conn.cursor()
        
        cur.execute("UPDATE checkouts SET return_date = DATE('now') WHERE id = ?", (checkout_id,))
        conn.commit()
        conn.close()
    
    # Run database update in background thread
    thread = threading.Thread(target=update_database)
    thread.daemon = True
    thread.start()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':

        return {'success': True}
    else:

        return redirect(redirect_url)

# ============================================================================
# DATABASE INITIALIZATION AND UTILITIES
# ============================================================================

def check_setup(data_path):
    """Initialize database tables if they don't exist"""
    conn = sqlite3.connect(str(data_path))
    cur = conn.cursor()
    
    try:
        cur.executescript('''
    CREATE TABLE IF NOT EXISTS classes (
        id INTEGER PRIMARY KEY,
        teacher_name TEXT,
        name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS students (
        id INTEGER PRIMARY KEY,
        fax_id TEXT UNIQUE,
        name TEXT NOT NULL,
        class_id INTEGER,
        FOREIGN KEY (class_id) REFERENCES classes(id)
    );

    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY,
        localnumber TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        subtitle TEXT,
        author TEXT NOT NULL,
        call1 TEXT,
        call2 TEXT,
        publisher TEXT,
        published TEXT,
        isbn TEXT,
        booklocation TEXT,
        cover_image BLOB
    );

    CREATE TABLE IF NOT EXISTS checkouts (
        id INTEGER PRIMARY KEY,
        student_id INTEGER NOT NULL,
        book_id INTEGER NOT NULL,
        checkout_date DATE DEFAULT CURRENT_DATE,
        return_date DATE,
        FOREIGN KEY (student_id) REFERENCES students(id),
        FOREIGN KEY (book_id) REFERENCES books(id)
    );

    CREATE TABLE IF NOT EXISTS uauth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS uauth_cookies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        cookie TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES uauth(id)
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(
        title, subtitle, author, localnumber, booklocation,
        tokenize = 'trigram'
    );

    CREATE TRIGGER IF NOT EXISTS books_ad AFTER DELETE ON books BEGIN
        DELETE FROM books_fts WHERE localnumber = OLD.localnumber;
    END;

    CREATE TRIGGER IF NOT EXISTS books_as AFTER INSERT ON books BEGIN
        INSERT INTO books_fts (title, subtitle, author, localnumber, booklocation)
        VALUES (NEW.title, NEW.subtitle, NEW.author, NEW.localnumber, NEW.booklocation);
    END;

    CREATE TRIGGER IF NOT EXISTS books_au AFTER UPDATE ON books BEGIN
        UPDATE books_fts SET
            title = NEW.title,
            subtitle = NEW.subtitle,
            author = NEW.author,
            booklocation = NEW.booklocation
        WHERE localnumber = NEW.localnumber;
    END;
    ''')
        conn.commit()
        print("[DB SETUP] Tables created successfully", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[DB SETUP] Error creating tables: {e}", file=sys.stderr, flush=True)
        conn.close()
        raise
    
    # Verify cover_image column exists
    cur.execute("PRAGMA table_info(books)")
    columns = [column[1] for column in cur.fetchall()]
    print(f"[DB SETUP] Books table columns: {columns}", file=sys.stderr, flush=True)
    
    if 'cover_image' not in columns:
        print("[DB SETUP] Adding missing cover_image column to books table", file=sys.stderr, flush=True)
        try:
            cur.execute("ALTER TABLE books ADD COLUMN cover_image BLOB")
            conn.commit()
            print("[DB SETUP] cover_image column added successfully", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[DB SETUP] Error adding cover_image column: {e}", file=sys.stderr, flush=True)
    
    conn.close()
    conn.close()

def check_database_validity(db_path, output_path=pathlib.Path(__file__).parent / 'books_missing_data.csv'):
    """Check for books with missing essential data and export to CSV"""
    conn = sqlite3.connect(str(db_path))
    try:
        query = """
            SELECT localnumber, title, author, call1, call2 
            FROM books 
            WHERE author IS NULL
            OR title IS NULL
            OR localnumber IS NULL
            OR call1 IS NULL;
        """
        
        df_missing = pandas.read_sql_query(query, conn)
        print(f"found {len(df_missing)} books with missing data")
        df_missing.to_csv(output_path, index=False)
    finally:
        conn.close()

def validate_database_schema(db_path):
    """Validate that the database has the correct schema structure"""
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Check critical tables and their columns
        schema_checks = [
            ('uauth', ['id', 'username', 'password']),
            ('uauth_cookies', ['id', 'user_id', 'cookie']),
            ('books', ['localnumber', 'title', 'author']),
            ('students', ['id', 'name', 'fax_id', 'class_id']),
            ('classes', ['id', 'name', 'teacher_name']),
            ('checkouts', ['id', 'student_id', 'book_id', 'checkout_date', 'return_date'])
        ]
        
        for table_name, required_columns in schema_checks:
            # Check if table exists
            cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cur.fetchone():
                conn.close()
                print(f"Table {table_name} does not exist")
                return False
            
            # Check if required columns exist
            cur.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cur.fetchall()]
            
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                conn.close()
                print(f"Table {table_name} is missing columns: {missing_columns}")
                return False
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Database validation failed: {e}")
        return False

def recreate_database_if_invalid(db_path):
    """Check database validity and recreate if corrupted or invalid"""
    import sys
    
    print(f"[DB CHECK] Starting database validation for {db_path}", file=sys.stderr, flush=True)
    
    # Check if database file exists
    if not db_path.exists():
        print(f"[DB CHECK] Database file does not exist, creating new database...", file=sys.stderr, flush=True)
        check_setup(db_path)
        print(f"[DB CHECK] New database created successfully", file=sys.stderr, flush=True)
        return
    
    try:
        # First, try to connect
        print(f"[DB CHECK] Testing database connection...", file=sys.stderr, flush=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute('SELECT 1')  # Basic query to test connection
        conn.close()
        print(f"[DB CHECK] Database connection successful", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[DB CHECK] Database connection failed: {e}", file=sys.stderr, flush=True)
        print(f"[DB CHECK] Deleting corrupted database and recreating...", file=sys.stderr, flush=True)
        try:
            db_path.unlink()
            print(f"[DB CHECK] Deleted corrupted database file", file=sys.stderr, flush=True)
        except Exception as delete_error:
            print(f"[DB CHECK] Failed to delete database: {delete_error}", file=sys.stderr, flush=True)
        check_setup(db_path)
        print(f"[DB CHECK] Database recreated successfully", file=sys.stderr, flush=True)
        return
    
    # Connection works, now validate schema
    print(f"[DB CHECK] Validating database schema...", file=sys.stderr, flush=True)
    if not validate_database_schema(db_path):
        print(f"[DB CHECK] Database schema is invalid or corrupted", file=sys.stderr, flush=True)
        print(f"[DB CHECK] Deleting and recreating database...", file=sys.stderr, flush=True)
        if db_path.exists():
            try:
                db_path.unlink()
                print(f"[DB CHECK] Deleted corrupted database at {db_path}", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"[DB CHECK] Failed to delete database: {e}", file=sys.stderr, flush=True)
                return
        check_setup(db_path)
        print(f"[DB CHECK] Database recreated successfully at {db_path}", file=sys.stderr, flush=True)
    else:
        print(f"[DB CHECK] Database schema is valid", file=sys.stderr, flush=True)

# ============================================================================
# APPLICATION STARTUP
# ============================================================================

# Verify database connectivity and schema, then initialize
recreate_database_if_invalid(db_path)
check_setup(db_path)

@app.route('/force_backup', methods=['POST'])
def force_backup():
    """Manual backup endpoint for user-initiated backups"""
    """Manual backup endpoint"""
    try:
        success = create_backup('manual', 'user_initiated')
        if success:
            return jsonify({'success': True, 'message': 'Backup created successfully'})
        else:
            return jsonify({'success': False, 'error': 'Backup failed'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/list_backups')
def list_backups():
    """Get list of available backup files with metadata"""
    """Get list of available backups"""
    try:
        backups = get_backup_list()
        return jsonify({'success': True, 'backups': backups})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/backup_status')
def backup_status():
    """Get backup system status and recent backup information"""
    """Get backup system status and recent backups"""
    try:
        backups = get_backup_list()
        
        # Group backups by type
        backup_counts = {'daily': 0, 'frequent': 0, 'events': 0, 'manual': 0}
        recent_backups = backups[:10]
        
        for backup in backups:
            backup_type = backup.get('backup_type', 'manual')
            if backup_type in backup_counts:
                backup_counts[backup_type] += 1
        
        status = {
            'enabled': BACKUP_ENABLED,
            'backup_directory': str(BACKUP_DIRECTORY),
            'total_backups': len(backups),
            'backup_counts': backup_counts,
            'recent_backups': recent_backups,
            'last_backup': backups[0] if backups else None
        }
        
        return jsonify({'success': True, 'status': status})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/restore_backup', methods=['POST'])
def restore_backup_route():
    """Restore database from a selected backup file"""
    """Restore database from backup"""
    try:
        backup_file = request.json.get('backup_file')
        if not backup_file:
            return jsonify({'success': False, 'error': 'No backup file specified'})
        
        success, message = restore_backup(backup_file)
        # Ensure schema is up-to-date after restore
        check_setup(db_path)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/clear_checkouts', methods=['POST'])
def clear_checkouts():
    """Clear all checkout records from the database"""
    """Clear all checkout records"""
    try:
        # Create backup before clearing checkouts
        trigger_event_backup('clear_all_checkouts')
        
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Get count of checkouts to be deleted
        cur.execute('SELECT COUNT(*) FROM checkouts')
        count = cur.fetchone()[0]
        
        # Delete all checkouts
        cur.execute('DELETE FROM checkouts')
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully cleared {count} checkout records'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # For development only - use gunicorn for production
    app.run(host='0.0.0.0', port=5000, debug=False) #, ssl_context=('CCL/nginx/ssl/selfsigned.crt', 'CCL/nginx/ssl/selfsigned.key'))

# WSGI entry point for production servers
application = app
