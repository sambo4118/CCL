import express from 'express';
import { eq, count } from 'drizzle-orm';
import { db } from '../database/index.js';
import { classes, students } from '../database/schema.js';
import multer from 'multer';

const upload = multer({ storage: multer.memoryStorage() });

const classesRoute = express.Router();

// GET /api/classes — list all classes with student counts
classesRoute.get('/', async (req, res) => {
    try {
        const rows = await db
            .select({
                id: classes.id,
                name: classes.name,
                teacherName: classes.teacherName,
                studentCount: count(students.id),
            })
            .from(classes)
            .leftJoin(students, eq(students.classId, classes.id))
            .groupBy(classes.id);

        res.json(rows);
    } catch (error) {
        console.error('Error listing classes:', error);
        res.status(500).json({ error: 'Failed to list classes' });
    }
});

// GET api/classes/:id get all the class details available
classesRoute.get('/:id', async (req, res) => {
    const classId = req.params.id;
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    let classInfo;
    try {
        classInfo = await db
            .select({
                id: classes.id,
                name: classes.name,
                teacherName: classes.teacherName,
                studentCount: count(students.id),
                classPhoto: classes.image,
            })
            .from(classes)
            .leftJoin(students, eq(students.classId, classes.id))
            .where(eq(classes.id, classId))
            .groupBy(classes.id)
            .get();

        if (!classInfo) {
            return res.status(404).json({ error: 'Class not found' });
        }
    } catch (error) {
        console.error('Error fetching class info:', error);
        return res.status(500).json({ error: 'Failed to fetch class info' });
    }

    let studentsInClass;
    try {
        studentsInClass = await db
            .select({
                id: students.id,
                name: students.name,
            })
            .from(students)
            .where(eq(students.classId, classId))
            .all();
    } catch (error) {
        console.error('Error fetching students in class:', error);
        return res.status(500).json({ error: 'Failed to fetch students in class' });
    }
    
    if (classInfo.image) {
        classInfo.image = `data:image/jpeg;base64,${classInfo.image.toString('base64')}`;
    }

    res.json({ class: classInfo, students: studentsInClass });
});

// PUT api/classes/:id 
classesRoute.put('/:id', upload.single('image'), async (req, res) => {
    const classId = req.params.id;
    
    console.log('--- Incoming PUT Request ---');
    console.log('Parsed text fields (req.body):', req.body);
    console.log('Parsed file (req.file):', req.file ? req.file.originalname : 'No file received');
    
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const { name, teacherName } = req.body;
    
    const updateData = {};
    
    if (name && name !== 'undefined' && name !== 'null') updateData.name = name;
    if (teacherName && teacherName !== 'undefined' && teacherName !== 'null') updateData.teacherName = teacherName;
    
    if (req.file) {
        updateData.image = req.file.buffer;
    }

    if (Object.keys(updateData).length === 0) {
        console.warn('Update aborted: No valid fields provided');
        return res.status(400).json({ error: 'No valid data provided to update' });
    }

    try {
        const updatedClass = await db 
            .update(classes)
            .set(updateData)
            .where(eq(classes.id, classId))
            .returning()
            .get();
            
        if (!updatedClass) return res.status(404).json({ error: 'Class not found' });

        res.json(updatedClass);

    } catch (error) {
        console.error('--- Drizzle DB Error ---');
        console.error(error);
        res.status(500).json({ error: 'Failed to update class in database' });
    }
});

export default classesRoute;
