SELECT 
  moron_nacional_pampa.universidad AS university,
  moron_nacional_pampa.carrerra AS career,
  moron_nacional_pampa.nombrre AS full_name,
  moron_nacional_pampa.sexo AS gender,
  CASE
    WHEN TO_DATE(moron_nacional_pampa.nacimiento,'DD/MM/YYYY') 
	  >= CURRENT_DATE - INTERVAL '16 years' 
        THEN (TO_DATE(moron_nacional_pampa.nacimiento,'DD/MM/YYYY') 
		  - INTERVAL '100 years')::date
    ELSE TO_DATE(moron_nacional_pampa.nacimiento,'DD/MM/YYYY')::date
  END AS birth_date,
  NULL AS "location",
  moron_nacional_pampa.codgoposstal AS "postal_code",
  moron_nacional_pampa.eemail AS email
FROM public.moron_nacional_pampa
WHERE moron_nacional_pampa.universidad = 'Universidad nacional de la pampa'
  AND TO_DATE(moron_nacional_pampa.fechaiscripccion,'DD/MM/YYYY')
    BETWEEN '2020-09-01' AND '2021-02-01'
UNION
SELECT 
  rio_cuarto_interamericana.univiersities AS university,
  rio_cuarto_interamericana.carrera AS career,
  rio_cuarto_interamericana.names AS full_name,
  rio_cuarto_interamericana.sexo AS gender,
  CASE
    WHEN TO_DATE(rio_cuarto_interamericana.fechas_nacimiento,'YY-Mon-DD') 
	  >= CURRENT_DATE - INTERVAL '16 years' 
        THEN (TO_DATE(rio_cuarto_interamericana.fechas_nacimiento,'YY-Mon-DD')
			- INTERVAL '100 years')::date
    ELSE TO_DATE(rio_cuarto_interamericana.fechas_nacimiento,'YY-Mon-DD')::date
  END AS birth_date,
  rio_cuarto_interamericana.localidad AS "location",
  NULL AS "postal_code",
  rio_cuarto_interamericana.email AS email
FROM public.rio_cuarto_interamericana
WHERE rio_cuarto_interamericana.univiersities = '-universidad-abierta-interamericana'
  AND TO_DATE(rio_cuarto_interamericana.inscription_dates,'DD/Mon/YY') 
    BETWEEN '2020-09-01' AND '2021-02-01'