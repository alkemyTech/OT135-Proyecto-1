SELECT universities AS university,careers AS career,names AS full_name,sexo AS sex,
CASE
 WHEN TO_DATE(birth_dates,'DD-MM-YYYY') >= CURRENT_DATE - INTERVAL '16 years' THEN TO_DATE(birth_dates,'DD-MM-YYYY') - INTERVAL '100 years'
 ELSE TO_DATE(birth_dates,'DD-MM-YYYY')
END AS birth_date,
NULL as postal_code,locations as location,emails
FROM lat_sociales_cine
WHERE universities = 'UNIVERSIDAD-DEL-CINE'
AND TO_DATE(inscription_dates,'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION ALL
SELECT universidades,carreras,nombres,sexo,
CASE
 WHEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') >= CURRENT_DATE - INTERVAL '16 years' THEN TO_DATE(fechas_nacimiento,'YY-Mon-DD') - INTERVAL '100 years'
 ELSE TO_DATE(fechas_nacimiento,'YY-Mon-DD')
END AS birth_date,
codigos_postales as postal_code,NULL as location,
emails
FROM uba_kenedy
WHERE universidades = 'universidad-de-buenos-aires'
AND TO_DATE(fechas_de_inscripcion,'YY-Mon-DD')
BETWEEN '2020-09-01' AND '2021-02-01'