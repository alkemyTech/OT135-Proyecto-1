SELECT mnp.universidad AS university,
mnp.carrerra AS career,
mnp.nombrre AS first_name,
mnp.sexo AS gender,
mnp.nacimiento AS age,
mnp.codgoposstal AS postal_code,
mnp.eemail AS email
FROM public.moron_nacional_pampa AS mnp
UNION
SELECT rci.univiersities AS university,
rci.carrera AS career,
rci.names AS name,
rci.sexo AS gender,
rci.fechas_nacimiento AS age,
rci.localidad AS location,
rci.email AS email
FROM public.rio_cuarto_interamericana AS rci
