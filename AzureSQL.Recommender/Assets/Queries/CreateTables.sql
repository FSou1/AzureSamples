CREATE TABLE Brands
(
    Id INT PRIMARY KEY IDENTITY,
    Name NVARCHAR(100) NOT NULL
);
CREATE TABLE Products
(
    Id INT PRIMARY KEY IDENTITY,
	Name NVARCHAR(100) NOT NULL,
	BrandId INT NOT NULL,
    FOREIGN KEY (BrandId) REFERENCES Brands (Id) ON DELETE CASCADE
);
CREATE TABLE People
(
    Id INT PRIMARY KEY IDENTITY,
	Name NVARCHAR(100) NOT NULL
);
CREATE TABLE Orders
(
    Id INT PRIMARY KEY IDENTITY,
	PersonId INT NOT NULL,
    FOREIGN KEY (PersonId) REFERENCES People (Id) ON DELETE CASCADE,
	ProductId INT NOT NULL,
    FOREIGN KEY (ProductId) REFERENCES Products (Id) ON DELETE CASCADE
);