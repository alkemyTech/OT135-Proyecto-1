SELECT mnp.universidad AS university,
mnp.carrerra AS career,
mnp.nombrre AS full_name,
mnp.sexo AS gender,
TO_DATE(mnp.nacimiento,'DD/MM/YYYY') AS date_of_birth,
NULL AS "location",
mnp.codgoposstal AS "postal_code",
mnp.eemail AS email
FROM public.moron_nacional_pampa AS mnp
WHERE mnp.universidad = 'Universidad nacional de la pampa'
	AND TO_DATE(mnp.fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION
SELECT rci.univiersities AS university,
rci.carrera AS career,
rci.names AS full_name,
rci.sexo AS gender,
--Bug: Toma todas las fechas que esten por debajo de 1970 como si fueran en los 2000
TO_DATE(rci.fechas_nacimiento,'YY/Mon/DD') AS date_of_birth,
rci.localidad AS "location",
NULL AS "postal_code",
rci.email AS email
FROM public.rio_cuarto_interamericana AS rci
WHERE rci.univiersities = '-universidad-abierta-interamericana'
	AND TO_DATE(rci.inscription_dates,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'
