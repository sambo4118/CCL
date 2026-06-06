import express from 'express';
import multer from 'multer';
import { eq } from 'drizzle-orm';
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

// GET /api/students — list all students with their class name
studentsRoute.get('/', async (req, res) => {
    try {
        const rows = await db
            .select({
                id: students.id,
                name: students.name,
                classId: students.classId,
                className: classes.name,
            })
            .from(students)
            .leftJoin(classes, eq(students.classId, classes.id));
        res.json(rows);
    } catch (error) {
        console.error('Error listing students:', error);
        res.status(500).json({ error: 'Failed to list students' });
    }
});

export default studentsRoute;
