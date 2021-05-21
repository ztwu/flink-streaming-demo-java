-- // Use the table function in SQL with LATERAL and TABLE keywords.
-- // CROSS JOIN a table function (equivalent to "join" in Table API).

SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)

-- // LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API).
SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE
