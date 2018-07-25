DECLARE @PersonId int = {0}, @MinCommonProductsCount int = {1};

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

/* Selecting people with common products */
PeopleWithCommonProducts_CTE AS
(
	SELECT
		PeopleWithSpecificProducts_CTE.PersonId,
		(
			/* PersonId-Amount. Number of common products between @PersonId and PersonId */
			SELECT SUM(Amount)
			FROM
			(	/* ProductId-Amount common products between @PersonId and PeopleWithSpecificProducts_CTE.PersonId */
				SELECT A.ProductId, (SELECT CASE WHEN A.Amount < B.Amount THEN A.Amount ELSE B.Amount END) AS Amount
				FROM PersonProducts_CTE AS A
				JOIN
				(   /* ProductId-Amount for the specified person */
					SELECT ProductId, COUNT(ProductId) AS Amount
					FROM Orders
					WHERE Orders.PersonId = PeopleWithSpecificProducts_CTE.PersonId
					GROUP BY ProductId
				) AS B
				ON A.ProductId = B.ProductId
			) AS CommonProducts
		) AS CommonProductsCount
	FROM PeopleWithSpecificProducts_CTE
),
/* Selecting people with more than @MinCommonProductsCount common products */
PeopleWithNCommonProducts_CTE AS
(
	SELECT *
	FROM PeopleWithCommonProducts_CTE
	WHERE CommonProductsCount >= @MinCommonProductsCount AND PersonId != @PersonId
)

SELECT DISTINCT Orders.ProductId AS ProductId
FROM Orders
JOIN PeopleWithNCommonProducts_CTE AS ProductsOwners
ON Orders.PersonId = ProductsOwners.PersonId
WHERE ProductId NOT IN (SELECT ProductId FROM PersonProducts_CTE)