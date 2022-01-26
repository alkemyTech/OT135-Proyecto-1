SELECT universidad as university, carrera as career, name as full_name, sexo as gender, fecha_nacimiento as date_of_birth, codigo_postal as postal_code, correo_electronico as email
FROM flores_comahue
WHERE (universidad = 'Univ. Nacional Del Comahue') AND (fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01')
UNION
SELECT universidad as university, carrera as career, nombre as full_name, sexo as gender, fecha_nacimiento as date_of_birth, localidad as location, email as email
FROM salvador_villa_maria
WHERE (universidad = 'Universidad Del Salvador') AND (fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01')