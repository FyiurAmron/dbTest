SELECT "shops"."shopName", "products"."productName" FROM "products"
INNER JOIN "productToShop" ON "products"."productId" = "productToShop"."productId"
INNER JOIN "shops" on "productToShop"."shopId" = "shops"."shopId"
WHERE length("productName") < 15
ORDER BY length("shopName"), length("productName")
;
