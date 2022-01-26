SELECT universities AS university,careers AS career,names AS full_name,sexo AS sex,birth_dates,locations as location_postal_code,emails
FROM lat_sociales_cine
WHERE universities='UNIVERSIDAD-DEL-CINE'
AND TO_DATE(inscription_dates,'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION ALL
SELECT universidades,carreras,nombres,sexo,fechas_nacimiento,codigos_postales,emails
FROM uba_kenedy
WHERE universidades='universidad-de-buenos-aires'
AND TO_DATE(fechas_de_inscripcion,'YY-Mon-DD') BETWEEN '2020-09-01' AND '2021-02-01'
ORDER BY university