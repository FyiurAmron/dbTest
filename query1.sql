SELECT "products"."productName","shops"."shopName", "cities"."cityName", "countries"."countryName" FROM "products"
INNER JOIN "productToShop" ON "products"."productId" = "productToShop"."productId"
INNER JOIN "shops" ON "productToShop"."shopId" = "shops"."shopId"
INNER JOIN "shopToCity" ON "shops"."shopId" = "shopToCity"."shopId"
INNER JOIN "cities" ON "shopToCity"."cityId" = "cities"."cityId"
INNER JOIN "countries" ON "cities"."countryId" = "countries"."countryId"
WHERE "products"."productId" IN
(
	SELECT "products"."productId"
	FROM "products"
	WHERE LENGTH("productName") < 15
)
ORDER BY length("productName"), length("shopName"), length("cityName"), length("countryName")
;
