SELECT "cities"."cityName" FROM "cities"
INNER JOIN "countries" ON "cities"."countryId" = "countries"."countryId"
WHERE substr("countryName", 1, 1) = 'A'
ORDER BY "cityName"
;