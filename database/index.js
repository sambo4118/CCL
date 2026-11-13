import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import path from "node:path";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import { fileURLToPath } from "node:url";
import * as schema from "./schema.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const dbPath = path.join(__dirname, "library.db");
export const sqlite = new Database(dbPath);
sqlite.pragma("journal_mode = DELETE");
sqlite.pragma("foreign_keys = ON");

export const db = drizzle(sqlite, { schema });

migrate(db, { migrationsFolder: path.join(__dirname, "migrations") });