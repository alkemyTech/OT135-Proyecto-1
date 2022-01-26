SELECT
v.nombre,
v.sexo,
v.fecha_nacimiento,
v.email,
v.fecha_de_inscripcion,
v.universidad,
v.carrera,
v.localidad
FROM public.salvador_villa_maria v
WHERE universidad = "villa maria"
AND fecha_de_inscripcion BETWEEN ""
UNION
SELECT
