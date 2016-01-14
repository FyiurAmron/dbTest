SELECT "cities"."cityName" FROM "cities"
WHERE "countryId" IN
(
	SELECT "countries"."countryId"
	FROM "countries"
	WHERE substr("countryName", 1, 1) = 'A'
)
ORDER BY "cityName"
;
