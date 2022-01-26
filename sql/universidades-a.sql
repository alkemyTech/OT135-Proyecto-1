SELECT
nombre,
sexo,
fecha_nacimiento,
email,
fecha_de_inscripcion,
universidad,
carrera,
localidad,
'null' AS codigo_postal
FROM public.salvador_villa_maria
WHERE universidad = "villa maria"
AND fecha_de_inscripcion BETWEEN '2020/09/20' AND '2021/02/01'
UNION
SELECT
name AS nombre,
sexo,
fecha_nacimiento,
correo_electronico AS email,
fecha_de_inscripcion,
universidad,
carrera,
'null' AS localidad,
codigo_postal
FROM public.flores_comahue f
WHERE universidad = "flores"
AND fecha_de_inscripcion BETWEEN '2020/09/20' AND '2021/02/01';