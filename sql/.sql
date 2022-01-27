SELECT universidad AS university, carrerra AS  career, nombrre AS "name", sexo AS gender,
nacimiento AS age, codgoposstal AS postal_code, 'null' AS "location", eemail AS email, 
to_date(fechaiscripccion,'DD/MM/YYYY') AS inscription_date 
FROM moron_nacional_pampa WHERE universidad = 'Universidad de morón' 
AND to_date(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION 
SELECT univiersities AS university, carrera AS career, "names" AS "name", sexo AS gender, fechas_nacimiento AS age,
'null' AS postal_code, localidad AS "location", email, to_date(inscription_dates,'DD/Mon/YY') AS inscription_date 
FROM rio_cuarto_interamericana WHERE univiersities = 'Universidad-nacional-de-río-cuarto' 
AND to_date(inscription_dates,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'