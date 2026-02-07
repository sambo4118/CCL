import sqlite3
import sys
import re
from pathlib import Path

def parse_enrollment_report(file_path, db_path='library.db'):
    """
    Parse the enrollment report format and import students into database.
    
    The report has structure like:
    Grade 01
    "Last, First"
    "Last, First"
    Female : 6
    Male : 1
    Total : 7
    
    Args:
        file_path (str): Path to the enrollment report file
        db_path (str): Path to the database file
        
    Returns:
        dict: Result with success status and message
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        students = []
        current_grade = None
        current_class_name = None
        student_id_counter = 1000  # Start fax_id from 1000
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines and headers
            if not line or line == "Enrollment Report":
                continue
                
            # Check for grade line (Grade XX or Grade K)
            grade_match = re.match(r'^Grade\s+(\w+)$', line)
            if grade_match:
                grade_code = grade_match.group(1)
                current_grade = grade_code
                
                # Convert grade code to proper class name
                if grade_code == 'K':
                    current_class_name = "Kindergarten"
                elif grade_code.isdigit():
                    grade_num = int(grade_code)
                    if grade_num == 1:
                        current_class_name = "1st Grade"
                    elif grade_num == 2:
                        current_class_name = "2nd Grade"
                    elif grade_num == 3:
                        current_class_name = "3rd Grade"
                    else:
                        current_class_name = f"{grade_num}th Grade"
                else:
                    current_class_name = f"Grade {grade_code}"
                continue
            
            # Check if line contains a quoted student name (must have comma)
            if line.startswith('"') and line.endswith('"') and ',' in line:
                if current_grade is None or current_class_name is None:
                    continue  # Skip students without grade assignment
                
                # Extract name from quotes and parse "Last, First" format
                name_content = line[1:-1]  # Remove quotes
                if ', ' in name_content:
                    last_name, first_name = name_content.split(', ', 1)
                    full_name = f"{first_name} {last_name}"
                    
                    students.append({
                        'name': full_name,
                        'fax_id': str(student_id_counter),
                        'class_name': current_class_name
                    })
                    student_id_counter += 1
            
            # Skip summary lines (Female :, Male :, Total :, Printed Date:)
            if any(keyword in line for keyword in ['Female :', 'Male :', 'Total :', 'Printed Date:']):
                continue
        
        if not students:
            return {
                'success': False,
                'error': 'No student data found in enrollment report'
            }
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("Clearing existing student and class data...")
        cursor.execute('DELETE FROM checkouts')  
        cursor.execute('DELETE FROM students')   
        cursor.execute('DELETE FROM classes')    
        
        
        print("Existing data cleared. Importing new data...")
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                fax_id TEXT UNIQUE NOT NULL,
                class_id INTEGER,
                FOREIGN KEY (class_id) REFERENCES classes(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS classes (
            id INTEGER PRIMARY KEY,
            teacher_name TEXT,
            name TEXT NOT NULL UNIQUE
            )
        ''')
        
        class_name_to_id = {}
        unique_class_names = set(student['class_name'] for student in students)
        
        for class_name in unique_class_names:
            cursor.execute(
                'INSERT INTO classes (name, teacher_name) VALUES (?, ?)',
                (class_name, '')  
            )
            class_name_to_id[class_name] = cursor.lastrowid
        
        inserted_count = 0
        errors = []
        
        for student in students:
            try:
                class_id = class_name_to_id.get(student['class_name'])
                cursor.execute(
                    'INSERT INTO students (name, fax_id, class_id) VALUES (?, ?, ?)',
                    (student['name'], student['fax_id'], class_id)
                )
                inserted_count += 1
            except Exception as e:
                errors.append(f"Error inserting {student['name']}: {str(e)}")
        
        conn.commit()
        conn.close()
        
        classes_created = len(unique_class_names)
        message = f"Data replacement completed: {inserted_count} students imported, {classes_created} classes created"
        
        result = {
            'success': True,
            'message': message,
            'inserted': inserted_count,
            'total_found': len(students),
            'classes_created': list(unique_class_names),
            'data_replaced': True
        }
        
        if errors:
            result['warnings'] = errors
        
        return result
        
    except FileNotFoundError:
        return {
            'success': False,
            'error': 'Enrollment report file not found'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Error processing enrollment report: {str(e)}'
        }

def import_students_from_csv(csv_file_path, db_path='library.db'):
    
    # check CSV formatting
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            first_lines = [file.readline().strip() for _ in range(10)]
        
        has_enrollment_header = any('Enrollment Report' in line for line in first_lines)
        has_grade_lines = any(re.match(r'^Grade\s+\w+', line) for line in first_lines)
        has_quoted_names = any(line.startswith('"') and line.endswith('"') and ',' in line for line in first_lines)
        
        if has_enrollment_header or (has_grade_lines and has_quoted_names):
            return parse_enrollment_report(csv_file_path, db_path)
    
    except Exception:
        pass
    
    return {
        'success': False,
        'error': 'File format not supported. Please use the enrollment report format or contact administrator.'
    }

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python import_students.py <file_path> [db_path]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else 'library.db'
    
    result = import_students_from_csv(file_path, db_path)
    
    if result['success']:
        print(f"SUCCESS: {result['message']}")
        if 'total_found' in result:
            print(f"Students found in report: {result['total_found']}")
        if 'classes_created' in result:
            print(f"Classes created: {', '.join(result['classes_created'])}")
        if 'warnings' in result:
            print("WARNINGS:")
            for warning in result['warnings']:
                print(f"  - {warning}")
    else:
        print(f"ERROR: {result['error']}")
        sys.exit(1)