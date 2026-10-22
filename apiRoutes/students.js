import express from 'express';
import multer from 'multer';
import { like, eq } from 'drizzle-orm';
import { db } from '../database/index.js';
import { students, classes } from '../database/schema.js';
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

export default studentsRoute;
