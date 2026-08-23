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
            })
            .from(students)
            .where(eq(students.id, studentId));
        
        if (studentInfo.length === 0) {
            return res.status(404).json({ error: 'Student Not Found' });
        }
        
        res.json(studentInfo[0])
    } catch (error) {
        console.error(error)
        return res.status(500).json({ error: 'Internal Server Error' });
    }

});

// GET /api/students — list all students or serach them by peram
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
            queryBuilder = queryBuilder
                .where(like(students.name, `%${searchQuery}%`))
                .limit(8);
        }

        const rows = await queryBuilder;
        res.json(rows);
    } catch (error) {
        console.error('Error listing students:', error);
        res.status(500).json({ error: 'Failed to list students' });
    }
});

export default studentsRoute;
