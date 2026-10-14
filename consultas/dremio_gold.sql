-- =====================================================================================
-- DREMIO · Consulta de la tabla Gold generada por el pipeline
--
-- Motor:    Dremio (http://localhost:9047, usuario/clave DREMIO_USER / DREMIO_PASSWORD del .env)
-- Fuente:   "nessie" (la crea scripts/configurar_dremio.sh) -> nessie.gold.vote_stats_per_post
-- Ejecutar: en el editor SQL de Dremio (botón "New Query"), una consulta a la vez.
--
-- Cada consulta tiene su par en consultas/trino_replica_gold.sql (misma numeración):
-- los resultados deben ser iguales -> el pipeline Silver -> Gold quedó correcto.
-- =====================================================================================


-- 1) TABLA GOLD (top 20 por votos; comparar con la consulta 1 de Trino) -------------------
SELECT post_id, post_type, anio_post, titulo, tags, owner_user_id, score_post,
       total_votos, votos_positivos, votos_negativos, votos_favorito, balance_votos,
       primer_voto, ultimo_voto, fecha_cargue
FROM nessie.gold.vote_stats_per_post
ORDER BY total_votos DESC, post_id
LIMIT 20;


-- 2) TOTALES (comparar con la consulta 2 de Trino) -------------------------------------
SELECT COUNT(*)              AS filas,
       SUM(total_votos)      AS votos,
       SUM(votos_positivos)  AS positivos,
       SUM(votos_negativos)  AS negativos
FROM nessie.gold.vote_stats_per_post;


-- 3) DETALLE POR AÑO Y TIPO DE POST (comparar con la consulta 3 de Trino) --------------
SELECT anio_post, post_type,
       COUNT(*)              AS posts,
       SUM(total_votos)      AS votos,
       SUM(votos_positivos)  AS positivos,
       SUM(votos_negativos)  AS negativos
FROM nessie.gold.vote_stats_per_post
GROUP BY anio_post, post_type
ORDER BY anio_post, post_type;


-- 4) UN REGISTRO PUNTUAL (comparar uno a uno con la consulta 4 de Trino) ---------------
SELECT post_id, post_type, anio_post, titulo, tags, total_votos, votos_positivos,
       votos_negativos, votos_favorito, primer_voto, ultimo_voto, fecha_cargue
FROM nessie.gold.vote_stats_per_post
WHERE post_id = 59837318;


-- 5) KPIs DE NEGOCIO SOBRE GOLD (extra) ------------------------------------------------
-- Tags con más votos positivos en preguntas (se toma el primer tag de cada pregunta)
SELECT SPLIT_PART(tags, '|', 1)  AS tag_principal,
       COUNT(*)                  AS preguntas,
       SUM(votos_positivos)      AS positivos,
       SUM(votos_negativos)      AS negativos,
       SUM(balance_votos)        AS balance
FROM nessie.gold.vote_stats_per_post
WHERE post_type = 'pregunta'
GROUP BY SPLIT_PART(tags, '|', 1)
ORDER BY positivos DESC
LIMIT 15;
