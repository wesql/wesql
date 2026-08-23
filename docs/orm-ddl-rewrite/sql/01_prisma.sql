-- Prisma 5/6 official MySQL getting-started (User/Post with relation).
-- prisma migrate default charset/collation: utf8mb4 / utf8mb4_unicode_ci
-- Source shape: https://www.prisma.io/docs/getting-started

CREATE TABLE `User` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `email` VARCHAR(191) NOT NULL,
    `name` VARCHAR(191) NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE=SMARTENGINE;

CREATE UNIQUE INDEX `User_email_key` ON `User`(`email`);

CREATE TABLE `Post` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `title` VARCHAR(191) NOT NULL,
    `content` VARCHAR(191) NULL,
    `published` BOOLEAN NOT NULL DEFAULT false,
    `authorId` INTEGER NOT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE=SMARTENGINE;

CREATE INDEX `Post_authorId_idx` ON `Post`(`authorId`);

ALTER TABLE `Post` ADD CONSTRAINT `Post_authorId_fkey` FOREIGN KEY (`authorId`) REFERENCES `User`(`id`) ON DELETE RESTRICT ON UPDATE CASCADE;
