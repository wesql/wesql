-- Isolated SmartEngine probes. Each statement is recorded independently.
-- Not an ORM default; used to label hard-fail classes.

-- P1 foreign key
CREATE TABLE p1_parent (
  id INT NOT NULL PRIMARY KEY
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE p1_child (
  id INT NOT NULL PRIMARY KEY,
  parent_id INT NOT NULL,
  CONSTRAINT p1_child_fk FOREIGN KEY (parent_id) REFERENCES p1_parent(id)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- P2 unique index on utf8mb4_unicode_ci (Laravel/Prisma default collation)
CREATE TABLE p2_unicode_ci_unique (
  id INT NOT NULL PRIMARY KEY,
  email VARCHAR(191) NOT NULL,
  UNIQUE KEY email (email)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- P3 unique index on utf8mb4_0900_ai_ci (MySQL 8 default, in SE allowlist)
CREATE TABLE p3_0900_ai_ci_unique (
  id INT NOT NULL PRIMARY KEY,
  email VARCHAR(191) NOT NULL,
  UNIQUE KEY email (email)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- P4 unique index on utf8mb4_general_ci (in SE allowlist)
CREATE TABLE p4_general_ci_unique (
  id INT NOT NULL PRIMARY KEY,
  email VARCHAR(191) NOT NULL,
  UNIQUE KEY email (email)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- P5 fulltext
CREATE TABLE p5_fulltext (
  id INT NOT NULL PRIMARY KEY,
  body TEXT,
  FULLTEXT KEY ft_body (body)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- P6 spatial / geometry
CREATE TABLE p6_spatial (
  id INT NOT NULL PRIMARY KEY,
  g GEOMETRY NOT NULL,
  SPATIAL INDEX (g)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- P7 generated column
CREATE TABLE p7_generated (
  id INT NOT NULL PRIMARY KEY,
  a INT NOT NULL,
  b INT AS (a + 1) STORED
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- P8 SERIALIZABLE is runtime, not DDL. Record SET result separately.
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
