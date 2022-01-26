SELECT * FROM moron_nacional_pampa WHERE to_date(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01'
UNION
SELECT * FROM rio_cuarto_interamericana WHERE to_date(inscription_dates,'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01'