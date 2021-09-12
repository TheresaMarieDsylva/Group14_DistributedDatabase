CREATE TABLE student ( studentId int not null PRIMARY KEY, name varchar(255) Not Null, marks int, age int NOT NULL CONSTRAINT FK_Teacher FOREIGN KEY (teacherId) REFERENCES teachers (teacherId) );
ALTER TABLE student ADD grade char(2);
DROP table order;
