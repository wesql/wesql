-- Extra cases required by design review. Expected behavior depends on
-- @@wesql_orm_ddl_rewrite (does not exist on current image; script records
-- current-engine behavior until the switch lands).

-- Unnamed FK
CREATE TABLE u_parent (id INT NOT NULL PRIMARY KEY) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE u_child (
  id INT NOT NULL PRIMARY KEY,
  parent_id INT NOT NULL,
  FOREIGN KEY (parent_id) REFERENCES u_parent(id)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Multiple FKs
CREATE TABLE m_a (id INT NOT NULL PRIMARY KEY) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE m_b (id INT NOT NULL PRIMARY KEY) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE m_child (
  id INT NOT NULL PRIMARY KEY,
  a_id INT NOT NULL,
  b_id INT NOT NULL,
  CONSTRAINT m_child_a FOREIGN KEY (a_id) REFERENCES m_a(id),
  CONSTRAINT m_child_b FOREIGN KEY (b_id) REFERENCES m_b(id)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Composite FK
CREATE TABLE c_parent (
  a INT NOT NULL,
  b INT NOT NULL,
  PRIMARY KEY (a, b)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE c_child (
  id INT NOT NULL PRIMARY KEY,
  a INT NOT NULL,
  b INT NOT NULL,
  CONSTRAINT c_child_fk FOREIGN KEY (a, b) REFERENCES c_parent(a, b)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- ALTER ADD FK
CREATE TABLE a_parent (id INT NOT NULL PRIMARY KEY) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
CREATE TABLE a_child (
  id INT NOT NULL PRIMARY KEY,
  parent_id INT NOT NULL
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
ALTER TABLE a_child ADD CONSTRAINT a_child_fk FOREIGN KEY (parent_id) REFERENCES a_parent(id);

-- ALTER add unique index on unicode_ci column
CREATE TABLE a_uni (
  id INT NOT NULL PRIMARY KEY,
  email VARCHAR(191) NOT NULL
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
ALTER TABLE a_uni ADD UNIQUE KEY email (email);

-- Non-indexed unicode_ci column must keep collation
CREATE TABLE keep_col (
  id INT NOT NULL PRIMARY KEY,
  note VARCHAR(191) COLLATE utf8mb4_unicode_ci
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- InnoDB FK must stay (other engine)
CREATE TABLE innodb_p (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE innodb_c (
  id INT NOT NULL PRIMARY KEY,
  p_id INT NOT NULL,
  CONSTRAINT innodb_c_fk FOREIGN KEY (p_id) REFERENCES innodb_p(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
