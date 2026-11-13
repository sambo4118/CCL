import express from 'express';
import multer from 'multer';
import { like, eq, count } from 'drizzle-orm';
import { db } from '../database/index.js';
import { students, classes, checkouts } from '../database/schema.js';
import { importStudents } from '../services/importStudents.js';

const studentsRoute = express.Router();

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 }, // 5MB
});

// POST /api/students/import — bulk import from enrollment report CSV
studentsRoute.post('/import', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    try {
        const result = await importStudents(req.file);
        res.json({ success: true, result });
    } catch (error) {
        console.error('Error importing students:', error);
        res.status(500).json({ error: 'Failed to import students', details: error.message });
    }
});

// GET /api/students/count — count all students (public, no auth required)
studentsRoute.get('/count', async (req, res) => {
    try {
        const [result] = await db.select({ count: count() }).from(students);
        res.json({ count: Number(result?.count ?? 0) });
    } catch (error) {
        console.error('Error counting students:', error);
        res.status(500).json({ error: 'Failed to count students' });
    }
});

// PUT /api/students/class/:id
studentsRoute.put('/class/:id', async (req, res) => {
    const studentId = req.params.id;
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    
    let updateData = {};
    updateData.classId = req.body.classId ?? req.body;

    try {
        const updatedStudent = await db
            .update(students)
            .set(updateData)
            .where(eq(students.id, studentId))
            .returning()
            .get();

        if (!updatedStudent) return res.status(404).json({ error: 'Student not found' });

        res.json(updatedStudent);
    } catch (error) {
        return res.status(404).json({ error: 'Failed to save student class reference' });
    }
});

// GET /api/students/:id — get all info on student with Id
studentsRoute.get('/:id', async (req, res) => {
    const studentId = req.params.id;
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });    
    try {
        const studentInfo = await db
            .select({
                id: students.id,
                name: students.name,
                classId: students.classId,
                className: classes.name,
                teacherName: classes.teacherName,
                age: students.age,
                enrollmentdate: students.enrollmentdate,
                contact: students.contact,
                notes: students.notes,
                photo: students.photo,
            })
            .from(students)
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(eq(students.id, studentId));
        
        if (studentInfo.length === 0) {
            return res.status(404).json({ error: 'Student Not Found' });
        }

        const student = { ...studentInfo[0] };
        if (student.photo) {
            student.photo = `data:image/jpeg;base64,${Buffer.from(student.photo).toString('base64')}`;
        }
        student.grade = student.className ?? null;
        student.homeroom = student.teacherName ?? null;
        
        res.json(student);
    } catch (error) {
        console.error(error);
        return res.status(500).json({ error: 'Internal Server Error' });
    }
});

// PUT /api/students/:id — update student info
studentsRoute.put('/:id', upload.single('photo'), async (req, res) => {
    const studentId = req.params.id;
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const { name, age, enrollmentdate, contact, notes, classId } = req.body;
    const updateData = {};

    if (name !== undefined && name !== 'undefined' && name.trim() !== '') {
        updateData.name = name.trim();
    }
    if (age !== undefined && age !== 'undefined') {
        updateData.age = age === '' || age === 'null' ? null : Number(age);
    }
    if (enrollmentdate !== undefined && enrollmentdate !== 'undefined') {
        updateData.enrollmentdate = enrollmentdate === '' || enrollmentdate === 'null' ? null : enrollmentdate;
    }
    if (contact !== undefined && contact !== 'undefined') {
        updateData.contact = contact === '' || contact === 'null' ? null : contact;
    }
    if (notes !== undefined && notes !== 'undefined') {
        updateData.notes = notes === '' || notes === 'null' ? null : notes;
    }
    if (classId !== undefined && classId !== 'undefined') {
        updateData.classId = classId === '' || classId === 'null' || classId === null ? null : Number(classId);
    }
    if (req.file) {
        updateData.photo = req.file.buffer;
    }

    if (Object.keys(updateData).length === 0) {
        return res.status(400).json({ error: 'No valid data provided to update' });
    }

    try {
        const updatedStudent = await db
            .update(students)
            .set(updateData)
            .where(eq(students.id, studentId))
            .returning()
            .get();

        if (!updatedStudent) {
            return res.status(404).json({ error: 'Student not found' });
        }

        return res.json(updatedStudent);
    } catch (error) {
        console.error('Error updating student:', error);
        return res.status(500).json({ error: 'Failed to update student' });
    }
});

// GET /api/students — list all students or search them by param
studentsRoute.get('/', async (req, res) => {
    const searchQuery = req.query.search;
    
    try {
        let queryBuilder = db
            .select({
                id: students.id,
                name: students.name,
                classId: students.classId,
                className: classes.name,
            })
            .from(students)
            .leftJoin(classes, eq(students.classId, classes.id));

        if (searchQuery) {
            const limit = Math.min(Number(req.query.limit) || 20, 50);
            queryBuilder = queryBuilder
                .where(like(students.name, `%${searchQuery}%`))
                .limit(limit);
        }

        const rows = await queryBuilder;
        res.json(rows);
    } catch (error) {
        console.error('Error listing students:', error);
        res.status(500).json({ error: 'Failed to list students' });
    }
});

// POST /api/students — create a new student
studentsRoute.post('/', upload.single('photo'), async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const { name, age, enrollmentdate, contact, notes, classId } = req.body;
    if (!name || name === 'undefined' || name === 'null' || !name.trim()) {
        return res.status(400).json({ error: 'Student name is required' });
    }

    const insertData = {
        name: name.trim(),
        age: (age !== undefined && age !== 'undefined' && age !== '' && age !== 'null' && !Number.isNaN(Number(age))) ? Number(age) : null,
        enrollmentdate: (enrollmentdate && enrollmentdate !== 'undefined' && enrollmentdate !== 'null') ? enrollmentdate.trim() : null,
        contact: (contact && contact !== 'undefined' && contact !== 'null') ? contact.trim() : null,
        notes: (notes && notes !== 'undefined' && notes !== 'null') ? notes.trim() : null,
        classId: (classId !== undefined && classId !== 'undefined' && classId !== '' && classId !== 'null' && classId !== null && !Number.isNaN(Number(classId))) ? Number(classId) : null,
    };

    if (req.file) {
        insertData.photo = req.file.buffer;
    }

    try {
        const newStudent = await db
            .insert(students)
            .values(insertData)
            .returning()
            .get();

        return res.status(201).json(newStudent);
    } catch (error) {
        console.error('Error creating student:', error);
        return res.status(500).json({ error: 'Failed to create student' });
    }
});

// DELETE /api/students/:id — delete a student
studentsRoute.delete('/:id', async (req, res) => {
    const studentId = Number(req.params.id);
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    if (!Number.isFinite(studentId)) return res.status(400).json({ error: 'Invalid student ID' });

    try {
        const deletedStudent = await db.transaction((transaction) => {
            const existing = transaction
                .select({ id: students.id, name: students.name })
                .from(students)
                .where(eq(students.id, studentId))
                .get();

            if (!existing) return null;

            transaction
                .delete(checkouts)
                .where(eq(checkouts.studentId, studentId))
                .run();

            transaction
                .delete(students)
                .where(eq(students.id, studentId))
                .run();

            return existing;
        });

        if (!deletedStudent) {
            return res.status(404).json({ error: 'Student not found' });
        }

        return res.json({ success: true, deleted: deletedStudent });
    } catch (error) {
        console.error('Error deleting student:', error);
        return res.status(500).json({ error: 'Failed to delete student' });
    }
});

export default studentsRoute;
