DECLARE @PersonId int = {0};

WITH
/* Products of the specified (@PersonId) person */
PersonProducts_CTE AS
(
	SELECT DISTINCT ProductId
	FROM Orders
	WHERE Orders.PersonId = @PersonId
),
/* People who have specific (PersonProducts_CTE) products */
PeopleWithSpecificProducts_CTE AS
(
	SELECT DISTINCT Orders.PersonId AS PersonId
	FROM Orders
	JOIN PersonProducts_CTE AS PersonProducts
	ON PersonProducts.ProductId = Orders.ProductId
),
/* Products of specific (PeopleWithSpecificProducts_CTE) people */
ProductsOfPeople_CTE AS
(
	SELECT DISTINCT Orders.ProductId AS ProductId
	FROM Orders
	JOIN PeopleWithSpecificProducts_CTE AS ProductsOwners
	ON Orders.PersonId = ProductsOwners.PersonId
)

/* Selecting people (ProductsOfPeople_CTE) products excluding person (@PersonId) products */
SELECT *
FROM ProductsOfPeople_CTE A
WHERE A.ProductId NOT IN (SELECT * FROM PersonProducts_CTE)
