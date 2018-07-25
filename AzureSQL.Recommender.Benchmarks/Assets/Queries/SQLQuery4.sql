DECLARE @PersonId int = {0}, @MinBrandProductsCount int = {1};

WITH
/* Products of the specified (@PersonId) person */
PersonProducts_CTE AS
(
	SELECT ProductId, COUNT(ProductId) AS Amount
	FROM Orders
	WHERE PersonId = @PersonId
	GROUP BY ProductId
),
PersonProductsWithBrands_CTE AS
(
	SELECT Products.BrandId, COUNT(ProductId) AS ProductsCount
	FROM PersonProducts_CTE
	JOIN Products
	ON ProductId = Products.Id
	GROUP BY BrandId
),
PersonTopBrands_CTE AS
(
	SELECT BrandId
	FROM PersonProductsWithBrands_CTE
	WHERE ProductsCount > @MinBrandProductsCount
)

SELECT Id AS ProductId
FROM PersonTopBrands_CTE A
JOIN Products B
ON A.BrandId = B.BrandId
