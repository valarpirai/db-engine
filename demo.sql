-- SimpleDB Demo Script
-- Run with: python -m db_engine.main --file demo.sql --data-dir ./demo_data

-- Create a users table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT,
    email TEXT,
    created_at BIGINT
);

-- Insert sample data
INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com', 1704067200);
INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com', 1704153600);
INSERT INTO users VALUES (3, 'Charlie', 22, 'charlie@example.com', 1704240000);
INSERT INTO users VALUES (4, 'Diana', 28, 'diana@example.com', 1704326400);
INSERT INTO users VALUES (5, 'Eve', 35, 'eve@example.com', 1704412800);

-- Query all users
SELECT * FROM users;

-- Query with WHERE clause
SELECT name, age FROM users WHERE age > 25;

-- Create index
CREATE INDEX idx_age ON users (age);

-- Query with ORDER BY
SELECT name, age FROM users ORDER BY age DESC;

-- Update data
UPDATE users SET age = 26 WHERE name = 'Alice';

-- Verify update
SELECT * FROM users WHERE name = 'Alice';

-- Delete data
DELETE FROM users WHERE age < 25;

-- Final count
SELECT * FROM users;

-- Analyze table
ANALYZE users;

-- Vacuum to reclaim space
VACUUM users;
