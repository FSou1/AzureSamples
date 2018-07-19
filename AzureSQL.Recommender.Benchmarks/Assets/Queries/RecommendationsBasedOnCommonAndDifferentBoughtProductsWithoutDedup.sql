DECLARE @PersonId int = {0};

WITH
/* Products of the specified person */
PersonProducts_CTE AS
(
	SELECT DISTINCT ProductId
	FROM Orders
	WHERE Orders.PersonId = @PersonId
),
/* People who have these products */
ProductsOwners_CTE AS
(
	SELECT DISTINCT Orders.PersonId AS PersonId
	FROM Orders
	JOIN PersonProducts_CTE AS PersonProducts
	ON PersonProducts.ProductId = Orders.ProductId
),
/* Products of people */
ProductsOfPeople_CTE AS
(
	SELECT Orders.ProductId AS ProductId
	FROM Orders
	JOIN (
		SELECT *
		FROM ProductsOwners_CTE A
		WHERE A.PersonId != @PersonId) AS ProductsOwners
	ON Orders.PersonId = ProductsOwners.PersonId
)

SELECT *
FROM ProductsOfPeople_CTE AS B
WHERE B.ProductId NOT IN (SELECT * FROM PersonProducts_CTE)