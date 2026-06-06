import { db } from '../database/index.js';
import { students, classes } from '../database/schema.js';

/**
 * Parses a school enrollment report like:
 *
 *   Enrollment Report
 *
 *   Grade 01
 *   "Avalos, Mila"
 *   "Blodgett, Margaret"
 *   ...
 *   Female : 6
 *   Male : 1
 *   Total : 7
 *
 *   Grade 02
 *   ...
 *
 * and returns { grade, name } records (name reordered to "First Last").
 */
function parseEnrollmentReport(text) {
    const lines = text.replace(/\r\n/g, '\n').split('\n');
    const records = [];
    let currentGrade = null;

    const gradeRe = /^\s*Grade\s+(\S+)\s*$/i;
    const totalsRe = /^\s*(Female|Male|Total)\s*:\s*\d+\s*$/i;
    const printedRe = /^\s*Printed\s+Date\s*:/i;
    const reportRe = /^\s*Enrollment\s+Report\s*$/i;

    for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) continue;
        if (reportRe.test(line)) continue;
        if (totalsRe.test(line)) continue;
        if (printedRe.test(line)) continue;

        const gradeMatch = line.match(gradeRe);
        if (gradeMatch) {
            currentGrade = normalizeGrade(gradeMatch[1]);
            continue;
        }

        if (!currentGrade) continue;

        const name = stripQuotes(line);
        if (!name) continue;

        records.push({ grade: currentGrade, name: reorderName(name) });
    }

    return records;
}

function stripQuotes(value) {
    const trimmed = String(value).trim();
    if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
        // Unescape doubled quotes per CSV convention
        return trimmed.slice(1, -1).replace(/""/g, '"').trim();
    }
    return trimmed;
}

function normalizeGrade(raw) {
    const value = String(raw).trim().toUpperCase();
    if (value === 'K' || value === 'KINDERGARTEN') return 'Grade K';
    // "01" -> "Grade 1", "12" -> "Grade 12"
    const num = Number(value);
    if (Number.isFinite(num)) return `Grade ${num}`;
    return `Grade ${value}`;
}

function reorderName(name) {
    // "Last, First" -> "First Last"
    const commaIndex = name.indexOf(',');
    if (commaIndex === -1) return name;
    const last = name.slice(0, commaIndex).trim();
    const first = name.slice(commaIndex + 1).trim();
    if (!first) return last;
    if (!last) return first;
    return `${first} ${last}`;
}

export async function importStudents(file) {
    const extension = file.originalname.split('.').pop().toLowerCase();
    if (extension !== 'csv' && extension !== 'txt') {
        throw new Error(`Unsupported file type: ${extension}. Expected .csv or .txt`);
    }

    const text = file.buffer.toString('utf-8');
    const records = parseEnrollmentReport(text);

    if (records.length === 0) {
        throw new Error('No student rows found in import file.');
    }

    const uniqueGrades = [...new Set(records.map((r) => r.grade))];

    let importedCount = 0;
    let classCount = 0;

    db.transaction((tx) => {
        // Wipe existing students and classes (mirrors the books importer behavior).
        tx.delete(students).run();
        tx.delete(classes).run();

        // Insert one class per grade. teacherName is required by schema; leave blank for now.
        for (const grade of uniqueGrades) {
            tx.insert(classes).values({ name: grade, teacherName: '' }).run();
        }

        const classRows = tx.select({ id: classes.id, name: classes.name }).from(classes).all();
        const classIdByName = new Map(classRows.map((row) => [row.name, row.id]));
        classCount = classRows.length;

        const studentRows = records.map((r) => ({
            name: r.name,
            classId: classIdByName.get(r.grade),
        }));

        for (let i = 0; i < studentRows.length; i += 200) {
            const chunk = studentRows.slice(i, i + 200);
            tx.insert(students).values(chunk).run();
        }

        importedCount = studentRows.length;
    });

    console.log(`[importStudents] imported ${importedCount} students across ${classCount} classes`);

    return { importedCount, classCount };
}
