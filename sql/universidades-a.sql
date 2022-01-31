SELECT
nombre AS full_name,
sexo AS gender,
CASE
	WHEN TO_DATE(fecha_nacimiento,'DD-Mon-YY') >= CURRENT_DATE - INTERVAL '16 years' 
		THEN (TO_DATE(fecha_nacimiento,'DD-Mon-YY') - INTERVAL '100 years')::date
	ELSE TO_DATE(fecha_nacimiento,'DD-Mon-YY')::date
END AS birth_date,
email,
TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') AS inscription_date,
universidad AS university,
carrera AS career,
localidad AS location,
'null' AS postal_code
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION
SELECT
name AS full_name,
sexo AS gender,
TO_DATE(fecha_nacimiento,'YYYY/MM/DD') AS birth_date,
correo_electronico AS email,
TO_DATE(fecha_de_inscripcion,'YYYY/MM/DD') AS inscription_date,
universidad AS university,
carrera AS career,
'null' AS location,
codigo_postal AS postal_code
FROM public.flores_comahue
WHERE universidad = 'UNIVERSIDAD DE FLORES'
AND TO_DATE(fecha_de_inscripcion,'YYYY/MM/DD') BETWEEN '2020/09/01' AND '2021/02/01';