CREATE TABLE `allowed_emails` (
	`id` integer PRIMARY KEY NOT NULL,
	`email` text NOT NULL,
	`note` text,
	`created_at` text DEFAULT CURRENT_TIMESTAMP NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `allowed_emails_email_unique` ON `allowed_emails` (`email`);--> statement-breakpoint
CREATE TABLE `authors` (
	`id` integer PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`age` integer,
	`book_count` integer
);
--> statement-breakpoint
CREATE UNIQUE INDEX `authors_name_unique` ON `authors` (`name`);--> statement-breakpoint
CREATE TABLE `books` (
	`id` integer PRIMARY KEY NOT NULL,
	`local_number` text NOT NULL,
	`title` text NOT NULL,
	`subtitle` text,
	`author_id` integer,
	`author` text,
	`call1` text,
	`call2` text,
	`publisher` text,
	`published` integer,
	`isbn` text,
	`booklocation` text,
	`cover` blob,
	`blurb` text,
	FOREIGN KEY (`author_id`) REFERENCES `authors`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `checkouts` (
	`id` integer PRIMARY KEY NOT NULL,
	`student_id` integer NOT NULL,
	`book_id` integer NOT NULL,
	`checkout_date` text DEFAULT CURRENT_TIMESTAMP NOT NULL,
	`return_date` text,
	FOREIGN KEY (`student_id`) REFERENCES `students`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`book_id`) REFERENCES `books`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `classes` (
	`id` integer PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`teacher_name` text NOT NULL,
	`image` blob
);
--> statement-breakpoint
CREATE TABLE `students` (
	`id` integer PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`class_id` integer NOT NULL,
	FOREIGN KEY (`class_id`) REFERENCES `classes`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `users` (
	`id` integer PRIMARY KEY NOT NULL,
	`google_id` text NOT NULL,
	`email` text NOT NULL,
	`name` text NOT NULL,
	`picture` text,
	`domain` text
);
--> statement-breakpoint
CREATE UNIQUE INDEX `users_google_id_unique` ON `users` (`google_id`);