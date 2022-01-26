SELECT
nombre,
sexo,
to_char("fecha_nacimiento"::date, 'YYYY-MM-DD') AS fecha_nacimiento,
email,
fecha_de_inscripcion,
universidad,
carrera,
localidad,
'null' AS codigo_postal
FROM public.salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
AND fecha_de_inscripcion BETWEEN '20-sep-01' AND '21-feb-01'
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
FROM public.flores_comahue
WHERE universidad = 'UNIVERSIDAD DE FLORES'
AND fecha_de_inscripcion BETWEEN '2020/09/01' AND '2021/02/01';


select
to_char("fecha_de_inscripcion"::date, 'YYYY-MM-DD') AS fecha_de_inscripcion
from public.salvador_villa_maria
where fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';

select
to_date("fecha_de_inscripcion"::date, 'YYYY-MM-DD') AS fecha_de_inscripcion
from public.salvador_villa_maria
where fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';

select
CAST("fecha_de_inscripcion"::date AS YYYY-MM-DD) AS fecha_de_inscripcion
from public.salvador_villa_maria
where fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01';


select * from public.salvador_villa_maria

AND fecha_de_inscripcion BETWEEN '20-sep-01' AND '21-feb-01'

