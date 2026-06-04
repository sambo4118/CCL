import { sql } from "drizzle-orm/sql";
import { sqliteTable, integer, text, blob } from "drizzle-orm/sqlite-core";

export const classes = sqliteTable("classes", {
    id: integer("id").primaryKey(),
    name: text("name").notNull(),
    teacherName: text("teacher_name").notNull(),
});

export const students = sqliteTable("students", {
    id: integer("id").primaryKey(),
    name: text("name").notNull(),
    classId: integer("class_id").notNull().references(() => classes.id),
});

export const authors = sqliteTable("authors", {
    id: integer("id").primaryKey(),
    name: text("name").notNull().unique(),
});

export const books = sqliteTable("books", {
    id: integer("id").primaryKey(),
    localNumber: text("local_number").notNull(),
    title: text("title").notNull(),
    subtitle: text("subtitle"),
    authorId: integer("author_id").references(() => authors.id),
    author: text("author"),
    call1: text("call1"),
    call2: text("call2"),
    publisher: text("publisher"),
    published: integer("published"),
    isbn: text("isbn"),
    bookLocation: text("booklocation"),
    coverImage: blob("cover", { mode: "buffer" }),
    blurb: text("blurb"),
});

export const checkouts = sqliteTable("checkouts", {
    id: integer("id").primaryKey(),
    studentId: integer("student_id").notNull().references(() => students.id),
    bookId: integer("book_id").notNull().references(() => books.id),
    checkoutDate: text("checkout_date").notNull().default(sql`CURRENT_TIMESTAMP`),
    returnDate: text("return_date"),
});

export const users = sqliteTable("users", {
    id: integer("id").primaryKey(),
    googleId: text("google_id").notNull().unique(),
    email: text("email").notNull(),
    name: text("name").notNull(),
    picture: text("picture"),
    domain: text("domain")
});