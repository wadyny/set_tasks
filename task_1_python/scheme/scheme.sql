CREATE TABLE room (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE student (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birthday DATE NOT NULL,
    sex CHAR(1) NOT NULL,
    room INT,
    FOREIGN KEY (room) REFERENCES room(id)
);

CREATE INDEX id_student_room ON student(room)