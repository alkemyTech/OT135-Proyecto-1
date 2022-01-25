SELECT universidad, carrera, name, sexo, fecha_nacimiento, codigo_postal, correo_electronico
FROM flores_comahue
WHERE fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01'
UNION
SELECT universidad, carrera, nombre, sexo, fecha_nacimiento, localidad, email
FROM salvador_villa_maria
WHERE fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01'