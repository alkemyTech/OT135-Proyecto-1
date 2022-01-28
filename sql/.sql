SELECT universidad AS university, carrerra AS  career, nombrre AS "name", sexo AS gender,
to_date(nacimiento, 'DD/MM/YYYY')::date AS age, codgoposstal AS postal_code, 'null' AS "location", eemail AS email, 
to_date(fechaiscripccion,'DD/MM/YYYY') AS inscription_date 
FROM moron_nacional_pampa WHERE universidad = 'Universidad de morón' 
AND to_date(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION 
SELECT univiersities AS university, carrera AS career, "names" AS "name", sexo AS gender, 
CASE
	WHEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') >= CURRENT_DATE - INTERVAL '16 years' THEN (TO_DATE(fechas_nacimiento,'YY-Mon-DD') - INTERVAL '100 years')::date
	ELSE TO_DATE(fechas_nacimiento,'YY-Mon-DD')::date
END AS age,
'null' AS postal_code, localidad AS "location", email, to_date(inscription_dates,'DD/Mon/YY') AS inscription_date 
FROM rio_cuarto_interamericana WHERE univiersities = 'Universidad-nacional-de-río-cuarto' 
AND to_date(inscription_dates,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'