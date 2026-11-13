import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { sqlite, dbPath } from '../database/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const backupDir = path.resolve(__dirname, '../database/backups');

export const MAX_BACKUPS_PER_TYPE = 5;
const DEFAULT_INTERVAL_MINUTES = Number(process.env.BACKUP_INTERVAL_MINUTES) || 60;
export const AUTO_BACKUP_INTERVAL_MS = DEFAULT_INTERVAL_MINUTES * 60 * 1000;

const VALID_BACKUP_FILENAME = /^(auto|manual)-backup-(\d+)\.db$/;

function ensureBackupDir() {
    if (!fs.existsSync(backupDir)) {
        fs.mkdirSync(backupDir, { recursive: true });
    }
}

function parseBackupFile(filename) {
    const match = VALID_BACKUP_FILENAME.exec(filename);
    if (!match) return null;

    const fullPath = path.join(backupDir, filename);
    let stat;
    try {
        stat = fs.statSync(fullPath);
    } catch {
        return null;
    }

    const prefix = match[1];
    const parsedTime = Number(match[2]);
    const timestamp = Number.isFinite(parsedTime) ? parsedTime : stat.mtimeMs;
    const type = prefix === 'auto' ? 'automatic' : 'manual';

    return {
        filename,
        type,
        timestamp,
        createdAt: new Date(timestamp).toISOString(),
        size: stat.size
    };
}

function getBackupsByType(type) {
    ensureBackupDir();
    const prefix = type === 'automatic' ? 'auto-backup-' : 'manual-backup-';

    const files = fs.readdirSync(backupDir)
        .filter((f) => f.startsWith(prefix) && f.endsWith('.db'))
        .map(parseBackupFile)
        .filter(Boolean)
        .sort((a, b) => b.timestamp - a.timestamp);

    return files;
}

export function pruneBackups(type) {
    const backups = getBackupsByType(type);
    if (backups.length <= MAX_BACKUPS_PER_TYPE) {
        return backups;
    }

    const keep = backups.slice(0, MAX_BACKUPS_PER_TYPE);
    const toRemove = backups.slice(MAX_BACKUPS_PER_TYPE);

    for (const item of toRemove) {
        try {
            fs.unlinkSync(path.join(backupDir, item.filename));
        } catch (err) {
            console.error(`Failed to delete old backup ${item.filename}:`, err);
        }
    }

    return keep;
}

export function listBackups() {
    ensureBackupDir();
    const automatic = pruneBackups('automatic');
    const manual = pruneBackups('manual');

    return {
        automatic,
        manual,
        intervalMinutes: DEFAULT_INTERVAL_MINUTES
    };
}

export async function createBackup(type = 'manual') {
    ensureBackupDir();
    const normalizedType = type === 'automatic' ? 'automatic' : 'manual';
    const prefix = normalizedType === 'automatic' ? 'auto-backup-' : 'manual-backup-';
    const timestamp = Date.now();
    const filename = `${prefix}${timestamp}.db`;
    const targetPath = path.join(backupDir, filename);

    await sqlite.backup(targetPath);
    pruneBackups(normalizedType);

    return parseBackupFile(filename);
}

export async function loadBackup(filename) {
    if (typeof filename !== 'string' || path.basename(filename) !== filename || !VALID_BACKUP_FILENAME.test(filename)) {
        throw new Error('Invalid backup filename');
    }

    ensureBackupDir();
    const sourcePath = path.join(backupDir, filename);
    if (!fs.existsSync(sourcePath)) {
        throw new Error('Backup file not found');
    }

    const backupDb = new Database(sourcePath, { readonly: true });
    try {
        await backupDb.backup(dbPath);
    } finally {
        backupDb.close();
    }

    sqlite.pragma('foreign_keys = ON');
    return parseBackupFile(filename);
}

let autoBackupTimer = null;

export function startAutomaticBackups(intervalMs = AUTO_BACKUP_INTERVAL_MS) {
    if (autoBackupTimer) {
        clearInterval(autoBackupTimer);
    }

    const checkAndRunAutoBackup = async () => {
        try {
            const currentAuto = getBackupsByType('automatic');
            const latest = currentAuto[0];
            const now = Date.now();

            if (!latest || (now - latest.timestamp) >= intervalMs) {
                const created = await createBackup('automatic');
                console.log(`[Backup] Automatic backup created: ${created?.filename}`);
            }
        } catch (err) {
            console.error('[Backup] Failed to create automatic backup:', err);
        }
    };

    checkAndRunAutoBackup();

    autoBackupTimer = setInterval(async () => {
        try {
            const created = await createBackup('automatic');
            console.log(`[Backup] Periodic automatic backup created: ${created?.filename}`);
        } catch (err) {
            console.error('[Backup] Periodic automatic backup failed:', err);
        }
    }, intervalMs);

    if (typeof autoBackupTimer.unref === 'function') {
        autoBackupTimer.unref();
    }

    return autoBackupTimer;
}
