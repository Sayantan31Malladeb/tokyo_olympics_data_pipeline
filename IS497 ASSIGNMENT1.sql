-- 1.1: create and use database DVDRentals --

CREATE DATABASE IF NOT EXISTS DVDRentals
CHARACTER SET ascii
  COLLATE ascii_general_ci;

USE DVDRentals;



-- 1.(A) Create a Table -- 

CREATE TABLE MovieTypes
(
MTypeID VARCHAR(4) NOT NULL,
MTypeDescrip VARCHAR(30) NOT NULL,
PRIMARY KEY (MTypeID)
)
ENGINE = INNODB;


CREATE TABLE Studios
(
StudID VARCHAR(4) NOT NULL,
StudDescrip VARCHAR(40) NOT NULL,
PRIMARY KEY (StudID)
)
ENGINE = INNODB;


CREATE TABLE Ratings
(
RatingID VARCHAR(4) NOT NULL,
RatingDescrip VARCHAR(30) NOT NULL,
PRIMARY KEY (RatingID)
)
ENGINE = INNODB;


CREATE TABLE Formats
(
FormID VARCHAR(2) NOT NULL,
FormDescrip VARCHAR(15) NOT NULL,
PRIMARY KEY (FormID)
)
ENGINE = INNODB;


CREATE TABLE Status
(
StatID VARCHAR(3) NOT NULL,
StatDescrip VARCHAR(20) NOT NULL,
PRIMARY KEY (StatID)
)
ENGINE = INNODB;


CREATE TABLE Roles
(
RoleID VARCHAR(4) NOT NULL,
RoleDescrip VARCHAR(30) NOT NULL,
PRIMARY KEY (RoleID)
)
ENGINE = INNODB;


CREATE TABLE Participants
(
PartID SMALLINT NOT NULL AUTO_INCREMENT,
PartFN VARCHAR(20) NOT NULL,
PartMN VARCHAR(20) NULL,
PartLN VARCHAR(20) NULL,
PRIMARY KEY (PartID)
)
ENGINE = INNODB;


CREATE TABLE Employees
(
EmpID SMALLINT NOT NULL AUTO_INCREMENT,
EmpFN VARCHAR(20) NOT NULL,
EmpMN VARCHAR(20) NULL,
EmpLN VARCHAR(20) NOT NULL,
PRIMARY KEY (EmpID)
)
ENGINE = INNODB;


CREATE TABLE DVDs
(
DVDID SMALLINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
DVDName VARCHAR(60) NOT NULL,
NumDisks TINYINT NOT NULL DEFAULT 1,
YearRlsd YEAR NOT NULL,
MTypeID VARCHAR(4) NOT NULL,
StudID VARCHAR(4) NOT NULL,
RatingID VARCHAR(4) NOT NULL,
FormID CHAR(2) NOT NULL,
StatID CHAR(3) NOT NULL,
FOREIGN KEY (MTypeID) REFERENCES MovieTypes (MTypeID),
FOREIGN KEY (StudID) REFERENCES Studios (StudID),
FOREIGN KEY (RatingID) REFERENCES Ratings (RatingID),
FOREIGN KEY (FormID) REFERENCES Formats (FormID),
FOREIGN KEY (StatID) REFERENCES Status (StatID)
)
ENGINE = INNODB;


CREATE TABLE Customers
(
CustID SMALLINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
CustFN VARCHAR(20) NOT NULL,
CustMN VARCHAR(20) NULL,
CustLN VARCHAR(20) NOT NULL
)
ENGINE = INNODB;


CREATE TABLE DVDParticipant
(
DVDID SMALLINT NOT NULL,
PartID SMALLINT NOT NULL,
RoleID VARCHAR(4) NOT NULL,
PRIMARY KEY (DVDID,PartID,RoleID),
FOREIGN KEY (DVDID) REFERENCES DVDs (DVDID),
FOREIGN KEY (PartID) REFERENCES Participants (PartID),
FOREIGN KEY (RoleID) REFERENCES Roles (RoleID)
)
ENGINE = INNODB;


CREATE TABLE Orders
(
OrderID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
CustID SMALLINT NOT NULL,
EmpID SMALLINT NOT NULL,
FOREIGN KEY (CustID) REFERENCES Customers (CustID),
FOREIGN KEY (EmpID) REFERENCES Employees (EmpID)
)
ENGINE = INNODB;



CREATE TABLE Transactions
(
TransID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
OrderID INT NOT NULL,
DVDID SMALLINT NOT NULL,
DateOut DATE NOT NULL,
DateDue DATE NOT NULL,
DateIn DATE NOT NULL,
FOREIGN KEY (OrderID) REFERENCES Orders (OrderID),
FOREIGN KEY (DVDID) REFERENCES DVDs (DVDID)
)
ENGINE = INNODB;





-- 1.(B) Populate the Tables --

INSERT INTO Formats
VALUES ('f1', 'Widescreen');


INSERT INTO Formats (FormID, FormDescrip)
VALUES ('f2', 'Fullscreen');


INSERT INTO Roles
VALUES ('r101', 'Actor'),
('r102', 'Director'),
('r103', 'Producer'),
('r104', 'Executive Producer'),
('r105', 'Co-Producer'),
('r106', 'Assistant Producer'),
('r107', 'Screenwriter'),
('r108', 'Composer');
select * from Roles;

INSERT INTO MovieTypes
VALUES ('mt10', 'Action'),
('mt11', 'Drama'),
('mt12', 'Comedy'),
('mt13', 'Romantic Comedy'),
('mt14', 'Science Fiction/Fantasy'),
('mt15', 'Documentary'),
('mt16', 'Musical');


INSERT INTO Studios
VALUES ('s101', 'Universal Studios'),
('s102', 'Warner Brothers'),
('s103', 'Time Warner'),
('s104', 'Columbia Pictures'),
('s105', 'Paramount Pictures'),
('s106', 'Twentieth Century Fox'),
('s107', 'Merchant Ivory Production'),
('s108', 'Lions Gate'),
('s109', 'Rich Media');


INSERT INTO Ratings
VALUES ('NR', 'Not rated'),
('G', 'General audiences'),
('PG', 'Parental guidance suggested'),
('PG13', 'Parents strongly cautioned'),
('R', 'Under 17 requires adult'),
('X', 'No one 17 and under');


INSERT INTO Status 
VALUES ('s1', 'Checked out'),
('s2', 'Available'),
('s3', 'Damaged'),
('s4', 'Lost'),
('s5', 'On hold'),
('s6', 'In transit'),
('s7', 'Reserved');



INSERT INTO Participants (PartFN, PartMN, PartLN)
VALUES ('Sydney', NULL, 'Pollack'),
('Robert', NULL, 'Redford'),
('Meryl', NULL, 'Streep'),
('John', NULL, 'Barry'),
('Henry', NULL, 'Buck'),
('Humphrey', NULL, 'Bogart'),
('Danny', NULL, 'Kaye'),
('Rosemary', NULL, 'Clooney'),
('Irving', NULL, 'Berlin'),
('Michael', NULL, 'Curtiz'),
('Bing', NULL, 'Crosby');


INSERT INTO DVDs 
VALUES (NULL, 'Out of Africa', 1, 2000, 'mt11', 's101', 'PG', 'f1', 's1'),
(NULL, 'The Maltese Falcon', 1, 2000, 'mt11', 's103', 'NR', 'f1', 's2'),
(NULL, 'Amadeus', 1, 1997, 'mt11', 's103', 'PG', 'f1', 's2'),
(NULL, 'The Shawshank Redemption', 1, 1994, 'mt11', 's101', 'R', 'f1', 's3'),
(NULL, 'The Godfather', 2, 1972, 'mt12', 's101', 'R', 'f1', 's4'),
(NULL, 'Pulp Fiction', 1, 1994, 'mt11', 's104', 'R', 'f1', 's5'),
(NULL, 'The Dark Knight', 1, 2008, 'mt13', 's105', 'PG13', 'f1', 's6'),
(NULL, 'Inception', 1, 2010, 'mt11', 's106', 'PG13', 'f1', 's7');
select * from DVDs;

INSERT INTO DVDParticipant
VALUES (3, 1, 'r102'),
(3, 4, 'r108'),
(3, 1, 'r103'),
(3, 2, 'r101'),
(3, 3, 'r101'),
(4, 6, 'r101'),
(1, 8, 'r101'),
(1, 9, 'r108'),
(1, 10, 'r102'),
(1, 11, 'r101'),
(1, 7, 'r101'),
(2, 5, 'r107');


INSERT INTO Employees (EmpFN, EmpMN, EmpLN)
VALUES ('John', 'P.', 'Smith'),
('Robert', NULL, 'Schroader'),
('Mary', 'Marie', 'Michaels'),
('John', NULL, 'Laguci'),
('Rita', 'C.', 'Carter'),
('George', NULL, 'Brooks');


INSERT INTO Customers (CustFN, CustMN, CustLN)
VALUES ('Ralph', 'Frederick', 'Johnson'),
('Hubert', 'T.', 'Weatherby'),
('Anne', NULL, 'Thomas'),
('Mona', 'J.', 'Cavenaugh'),
('Peter', NULL, 'Taylor'),
('Ginger', 'Meagan', 'Delaney');


INSERT INTO Orders (CustID, EmpID)
VALUES (1, 3),
(1, 2),
(2, 5),
(3, 6),
(4, 1),
(3, 3),
(5, 2),
(6, 4),
(4, 5),
(6, 2),
(3, 1),
(1, 6),
(5, 4);



INSERT INTO Transactions (OrderID, DVDID, DateOut, DateDue, DateIn)
VALUES (1, 1, CURDATE(), CURDATE()+3, CURDATE()+7),
(1, 4, CURDATE(), CURDATE()+3, CURDATE()+7),
(1, 8, CURDATE(), CURDATE()+3, CURDATE()+7),
(2, 3, CURDATE(), CURDATE()+3, CURDATE()+7),
(3, 4, CURDATE(), CURDATE()+3, CURDATE()+7),
(3, 1, CURDATE(), CURDATE()+3, CURDATE()+7),
(3, 7, CURDATE(), CURDATE()+3, CURDATE()+7),
(4, 4, CURDATE(), CURDATE()+3, CURDATE()+7),
(5, 3, CURDATE(), CURDATE()+3, CURDATE()+7),
(6, 2, CURDATE(), CURDATE()+3, CURDATE()+7),
(6, 1, CURDATE(), CURDATE()+3, CURDATE()+7),
(7, 4, CURDATE(), CURDATE()+3, CURDATE()+7),
(8, 2, CURDATE(), CURDATE()+3, CURDATE()+7),
(8, 1, CURDATE(), CURDATE()+3, CURDATE()+7),
(8, 3, CURDATE(), CURDATE()+3, CURDATE()+7),
(9, 7, CURDATE(), CURDATE()+3, CURDATE()+7),
(9, 1, CURDATE(), CURDATE()+3, CURDATE()+7),
(10, 5, CURDATE(), CURDATE()+3, CURDATE()+7),
(11, 6, CURDATE(), CURDATE()+3, CURDATE()+7),
(11, 2, CURDATE(), CURDATE()+3, CURDATE()+7),
(11, 8, CURDATE(), CURDATE()+3, CURDATE()+7),
(12, 5, CURDATE(), CURDATE()+3, CURDATE()+7),
(13, 7, CURDATE(), CURDATE()+3, CURDATE()+7);





-- 1. (C) Table joins -- 

-- BASIC JOIN-- 
SELECT d.DVDName, mt.MTypeDescrip As MovieType
FROM DVDs AS d, MovieTypes AS mt
WHERE d.MTypeID=mt.MTypeID AND StatID='s2'
ORDER BY DVDName;


SELECT CONCAT_WS(' ', CustFN, CustMN, CustLN) AS Customer, TransID
FROM Customers INNER JOIN Orders USING (CustID)
INNER JOIN Transactions USING (OrderID)
WHERE DVDID=7
ORDER BY CustLN;


-- CROSS JOIN --
SELECT d.DVDName, mt.MTypeDescrip AS MovieType,
CONCAT(r.RatingID, ': ', r.RatingDescrip) AS Rating
FROM MovieTypes AS mt CROSS JOIN DVDs AS d USING (MTypeID)
CROSS JOIN Ratings AS r USING (RatingID)
WHERE StatID='s2'
ORDER BY DVDName;


-- OUTER JOIN -- 
SELECT CONCAT_WS(' ', EmpFN, EmpMN, EmpLN) AS Employee, OrderID
FROM Employees LEFT JOIN Orders USING (EmpID)
ORDER BY EmpLN;





-- 1.(D) Update Table --

UPDATE DVDs, Studios
SET DVDs.StatID='s2'
WHERE DVDs.StudID=Studios.StudID
AND Studios.StudDescrip='Metro-Goldwyn-Mayer';





-- 1.(E) Read Table --

SELECT EmpFN AS 'First Name', EmpLN AS 'Last Name'
FROM Employees;

SELECT OrderID, COUNT(*) AS Transactions
FROM Transactions
GROUP BY OrderID;
-- The query above gives us 2 columns, the OrderID and a count of how many times that OrderID was repeated in the transactions table.


SELECT OrderID, CustID
FROM Orders
ORDER BY OrderID DESC
LIMIT 1;
-- The Query above returns 2 columns , the OrderID and CustID from the orders table. It starts with the highest value of OrderID because of the
-- DESC function, and only the highest value is returned due to the LIMIT 1 clause.--




-- 1.(E) Delete From Tables -- 

DELETE FROM Studios WHERE StudID='s108' or StudID='s109';

SELECT * from Studios;
