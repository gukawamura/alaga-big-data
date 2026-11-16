-- ============================================================================
-- Alaga Alerta · Ouro · 5 consultas SQL (req. 07 — uma delas de IMPACTO)
-- Catálogo/esquema: workspace.gs
-- Cada consulta responde a uma pergunta que o MVP faz ao cidadão.
-- ============================================================================
USE workspace.gs;

-- ----------------------------------------------------------------------------
-- 1) OPERACIONAL — Risco atual por bairro (alimenta o mapa/cards do MVP)
-- Pergunta: "quais bairros estão em risco agora e quantas pessoas estão expostas?"
-- Interpretação: ordena os bairros do pior para o melhor no instante mais recente.
-- ----------------------------------------------------------------------------
SELECT b.bairro, f.data_hora, f.precipitacao_mm, f.nivel_risco, f.populacao_exposta
FROM fato_risco_bairro_hora f
JOIN dim_bairro b USING (bairro_id)
WHERE f.data_hora = (SELECT max(data_hora) FROM fato_risco_bairro_hora)
ORDER BY f.populacao_exposta DESC, f.precipitacao_mm DESC;

-- ----------------------------------------------------------------------------
-- 2) RANKING — Precipitação acumulada por bairro no período coletado
-- Pergunta: "onde choveu mais no total?"
-- Interpretação: prioriza bairros para inspeção/manutenção preventiva.
-- ----------------------------------------------------------------------------
SELECT b.bairro,
       round(sum(f.precipitacao_mm), 1)  AS chuva_acumulada_mm,
       round(max(f.precipitacao_mm), 1)  AS pico_horario_mm,
       count(*)                          AS horas_observadas
FROM fato_risco_bairro_hora f
JOIN dim_bairro b USING (bairro_id)
GROUP BY b.bairro
ORDER BY chuva_acumulada_mm DESC;

-- ----------------------------------------------------------------------------
-- 3) *** IMPACTO *** — População exposta a risco alto/crítico por bairro
-- Pergunta: "qual o alcance humano do problema?" (indicador de impacto p/ pitch/BI)
-- Interpretação: nº de horas em risco e o pico de pessoas expostas quantificam o impacto.
-- ----------------------------------------------------------------------------
SELECT b.bairro,
       b.populacao,
       count(*)                       AS horas_em_risco,
       max(f.populacao_exposta)       AS pico_populacao_exposta,
       sum(f.qtd_ocorrencias)         AS ocorrencias_cidadao
FROM fato_risco_bairro_hora f
JOIN dim_bairro b USING (bairro_id)
WHERE f.nivel_risco IN ('alto', 'critico')
GROUP BY b.bairro, b.populacao
ORDER BY pico_populacao_exposta DESC;

-- ----------------------------------------------------------------------------
-- 4) PADRÃO TEMPORAL — Distribuição dos níveis de risco por período do dia
-- Pergunta: "em que períodos o risco se concentra?"
-- Interpretação: ajuda a calibrar quando disparar alertas (manhã/tarde/noite).
-- ----------------------------------------------------------------------------
SELECT t.periodo,
       f.nivel_risco,
       count(*) AS ocorrencias
FROM fato_risco_bairro_hora f
JOIN dim_tempo t USING (data_hora)
GROUP BY t.periodo, f.nivel_risco
ORDER BY t.periodo, f.nivel_risco;

-- ----------------------------------------------------------------------------
-- 5) SÉRIE TEMPORAL — Evolução horária do risco de um bairro (gráfico do MVP)
-- Pergunta: "como o risco evoluiu nas últimas horas no meu bairro?"
-- Interpretação: série usada no detalhe do bairro na tela do cidadão.
-- ----------------------------------------------------------------------------
SELECT f.data_hora, f.precipitacao_mm, f.nivel_risco, f.qtd_ocorrencias
FROM fato_risco_bairro_hora f
JOIN dim_bairro b USING (bairro_id)
WHERE b.bairro_id = 'SP007'            -- Jardim Ângela (maior população)
ORDER BY f.data_hora;
