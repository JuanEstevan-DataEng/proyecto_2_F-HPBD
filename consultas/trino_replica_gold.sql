-- =====================================================================================
-- TRINO · Réplica de la tabla Gold a partir de las tablas históricas de Silver
--
-- Motor:    Trino (http://localhost:8085, catálogo "iceberg" -> Nessie + MinIO)
-- Ejecutar: DBeaver (conexión Trino, host localhost, puerto 8085, usuario admin, sin clave)
--           o en consola:  docker exec -it p2_trino trino
--
-- La consulta 1 hace, con SQL sobre Silver, la misma agregación que el job
-- jobs/gold_agg.py (SQL_GOLD) escribió en nessie.gold.vote_stats_per_post.
-- Su resultado debe ser IGUAL a la tabla Gold que se consulta en Dremio
-- (consultas/dremio_gold.sql): mismo número de filas y mismos valores.
-- =====================================================================================


-- 1) RÉPLICA DE GOLD DESDE SILVER --------------------------------------------------------
SELECT
    p.id                                                    AS post_id,
    p.post_type                                             AS post_type,
    p.anio                                                  AS anio_post,
    p.title                                                 AS titulo,
    p.tags                                                  AS tags,
    p.owner_user_id                                         AS owner_user_id,
    p.score                                                 AS score_post,
    COUNT(v.id)                                             AS total_votos,
    SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)     AS votos_positivos,
    SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END)     AS votos_negativos,
    SUM(CASE WHEN v.vote_type_id = 5 THEN 1 ELSE 0 END)     AS votos_favorito,
    SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)
      - SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END) AS balance_votos,
    MIN(v.creation_date)                                    AS primer_voto,
    MAX(v.creation_date)                                    AS ultimo_voto
FROM iceberg.silver.post_hist p
JOIN iceberg.silver.votes_hist v
  ON v.post_id = p.id
GROUP BY p.id, p.post_type, p.anio, p.title, p.tags, p.owner_user_id, p.score
ORDER BY total_votos DESC, post_id
LIMIT 20;


-- 2) TOTALES DE LA RÉPLICA (comparar con la consulta 2 de dremio_gold.sql) ---------------
WITH replica AS (
    SELECT
        p.id AS post_id, p.post_type, p.anio AS anio_post,
        COUNT(v.id)                                         AS total_votos,
        SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END) AS votos_positivos,
        SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END) AS votos_negativos
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.votes_hist v ON v.post_id = p.id
    GROUP BY p.id, p.post_type, p.anio
)
SELECT COUNT(*)              AS filas,
       SUM(total_votos)      AS votos,
       SUM(votos_positivos)  AS positivos,
       SUM(votos_negativos)  AS negativos
FROM replica;


-- 3) DETALLE POR AÑO Y TIPO DE POST (comparar con la consulta 3 de dremio_gold.sql) -------
SELECT p.anio AS anio_post, p.post_type,
       COUNT(DISTINCT p.id)                                 AS posts,
       COUNT(v.id)                                          AS votos,
       SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)  AS positivos,
       SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END)  AS negativos
FROM iceberg.silver.post_hist p
JOIN iceberg.silver.votes_hist v ON v.post_id = p.id
GROUP BY p.anio, p.post_type
ORDER BY anio_post, post_type;


-- 4) UN REGISTRO PUNTUAL (comparar uno a uno con la consulta 4 de dremio_gold.sql) --------
SELECT
    p.id AS post_id, p.post_type, p.anio AS anio_post, p.title AS titulo, p.tags,
    COUNT(v.id)                                             AS total_votos,
    SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)     AS votos_positivos,
    SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END)     AS votos_negativos,
    SUM(CASE WHEN v.vote_type_id = 5 THEN 1 ELSE 0 END)     AS votos_favorito,
    MIN(v.creation_date) AS primer_voto,
    MAX(v.creation_date) AS ultimo_voto
FROM iceberg.silver.post_hist p
JOIN iceberg.silver.votes_hist v ON v.post_id = p.id
WHERE p.id = 59837318   -- pregunta "Why is this \"Hello, World!\" JavaScript code fragment..."
GROUP BY p.id, p.post_type, p.anio, p.title, p.tags;


-- 5) PRUEBA AUTOMÁTICA: réplica vs Gold (Trino también lee Gold) -------------------------
-- Debe devolver 0 en las dos columnas: no hay filas en un lado que no estén en el otro.
WITH replica AS (
    SELECT
        p.id AS post_id, p.post_type, p.anio AS anio_post, p.title AS titulo, p.tags,
        p.owner_user_id, p.score AS score_post,
        COUNT(v.id)                                             AS total_votos,
        SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)     AS votos_positivos,
        SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END)     AS votos_negativos,
        SUM(CASE WHEN v.vote_type_id = 5 THEN 1 ELSE 0 END)     AS votos_favorito,
        SUM(CASE WHEN v.vote_type_id = 2 THEN 1 ELSE 0 END)
          - SUM(CASE WHEN v.vote_type_id = 3 THEN 1 ELSE 0 END) AS balance_votos,
        MIN(v.creation_date) AS primer_voto,
        MAX(v.creation_date) AS ultimo_voto
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.votes_hist v ON v.post_id = p.id
    GROUP BY p.id, p.post_type, p.anio, p.title, p.tags, p.owner_user_id, p.score
),
gold AS (
    SELECT post_id, post_type, anio_post, titulo, tags, owner_user_id, score_post,
           total_votos, votos_positivos, votos_negativos, votos_favorito, balance_votos,
           primer_voto, ultimo_voto
    FROM iceberg.gold.vote_stats_per_post
)
SELECT
    (SELECT COUNT(*) FROM (SELECT * FROM replica EXCEPT SELECT * FROM gold)) AS solo_en_replica,
    (SELECT COUNT(*) FROM (SELECT * FROM gold EXCEPT SELECT * FROM replica)) AS solo_en_gold;
