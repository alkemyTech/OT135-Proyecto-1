SELECT universidad as university, carrera as career, name as full_name, sexo as gender, fecha_nacimiento as date_of_birth, codigo_postal as postal_code, 'null' as location, correo_electronico as email
FROM flores_comahue
WHERE (universidad = 'UNIV. NACIONAL DEL COMAHUE') AND (TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT universidad as university, carrera as career, nombre as full_name, sexo as gender, fecha_nacimiento as date_of_birth, 'null' as postal_code, localidad as location, email as email
FROM salvador_villa_maria
WHERE (universidad = 'UNIVERSIDAD_DEL_SALVADOR') AND (TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020/09/01' AND '2021/02/01')