-- Drizzle ORM official MySQL getting-started (users + posts relation).
-- drizzle-kit mysql default: utf8mb4, no explicit collation → server default utf8mb4_0900_ai_ci on MySQL 8.
-- `.references()` emits FOREIGN KEY.

CREATE TABLE `users` (
  `id` int AUTO_INCREMENT NOT NULL,
  `email` varchar(255) NOT NULL,
  `name` varchar(255),
  CONSTRAINT `users_id` PRIMARY KEY(`id`),
  CONSTRAINT `users_email_unique` UNIQUE(`email`)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `posts` (
  `id` int AUTO_INCREMENT NOT NULL,
  `title` varchar(255) NOT NULL,
  `content` text,
  `author_id` int NOT NULL,
  CONSTRAINT `posts_id` PRIMARY KEY(`id`),
  CONSTRAINT `posts_author_id_users_id_fk` FOREIGN KEY (`author_id`) REFERENCES `users`(`id`) ON DELETE no action ON UPDATE no action
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
