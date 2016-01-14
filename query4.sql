SELECT "shops"."shopName", "products"."productName" FROM "products", "productToShop", "shops"
WHERE "shops"."shopId" = "productToShop"."shopId" AND "products"."productId" = "productToShop"."productId"
AND length("productName") < 15
ORDER BY length("shopName"), length("productName")
;
