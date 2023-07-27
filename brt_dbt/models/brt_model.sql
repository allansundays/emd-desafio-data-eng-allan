-- models/brt_data_model.sql

-- Transformações: exemplo de agrupamento de dados
-- SELECT
--     *
-- FROM brt_data

SELECT 
    bus_id AS id_onibus, 
    position AS posicao,
    speed AS velocidade
FROM brt_data