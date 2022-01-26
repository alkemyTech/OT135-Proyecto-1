SELECT mnp.universidad AS university,
mnp.carrerra AS career,
LEFT(mnp.nombrre, strpos(mnp.nombrre,' ') - 1) AS first_name,
RIGHT(mnp.nombrre, -(strpos(mnp.nombrre,' '))) AS last_name,
mnp.sexo AS gender,
(DATE_PART('year',CURRENT_DATE) - DATE_PART('year',TO_DATE(mnp.nacimiento,'DD/MM/YYYY')))::int AS age,
NULL AS "location",
mnp.codgoposstal AS "postal_code",
mnp.eemail AS email
FROM public.moron_nacional_pampa AS mnp
WHERE mnp.universidad = 'Universidad nacional de la pampa'
	AND TO_DATE(mnp.fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION
SELECT rci.univiersities AS university,
rci.carrera AS career,
LEFT(rci.names, strpos(rci.names,'-') - 1) AS first_name,
RIGHT(rci.names, -(strpos(rci.names,'-'))) AS last_name,
rci.sexo AS gender,
(DATE_PART('year',CURRENT_DATE) - DATE_PART('year',TO_DATE(rci.inscription_dates,'DD/Mon/YY')))::int AS age,
rci.localidad AS "location",
NULL AS "postal_code",
rci.email AS email
FROM public.rio_cuarto_interamericana AS rci
WHERE rci.univiersities = '-universidad-abierta-interamericana'
	AND TO_DATE(rci.inscription_dates,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'
