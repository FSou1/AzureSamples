DECLARE @PersonId int = {0};

WITH
/* Products of the specified (@PersonId) person */
PersonProducts_CTE AS
(
	SELECT ProductId, COUNT(ProductId) AS Amount
	FROM Orders
	WHERE PersonId = @PersonId
	GROUP BY ProductId
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
	SELECT DISTINCT Orders.ProductId AS ProductId, Orders.PersonId
	FROM Orders
	JOIN PeopleWithSpecificProducts_CTE AS ProductsOwners
	ON Orders.PersonId = ProductsOwners.PersonId
),
/* Grouping */
ProductsAndOwnersCount_CTE AS
(
	SELECT ProductId, COUNT(ProductId) AS OwnersCount
	FROM ProductsOfPeople_CTE
	GROUP BY ProductId
)

SELECT *
FROM ProductsAndOwnersCount_CTE
WHERE ProductId NOT IN (SELECT ProductId FROM PersonProducts_CTE)
ORDER BY OwnersCount
DESC
