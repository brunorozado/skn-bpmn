-- =====================================================
-- VIEW PARA EXTRAÇÃO DE LOGS DE EVENTOS - SANKHYA
-- Mapeador e Analisador de Processos com IA
-- =====================================================

-- Esta view extrai dados de logs de eventos do ERP Sankhya para análise de processos
-- utilizando técnicas de Process Mining. A view combina informações de diferentes
-- tabelas para criar um log de eventos estruturado.

CREATE OR REPLACE VIEW VW_PROCESS_MINING_EVENTS AS
SELECT 
    -- Identificador único do caso (processo)
    CAB.NUNOTA AS case_id,
    
    -- Nome da atividade executada
    CASE 
        WHEN CAB.STATUSNOTA = 'P' THEN 'Pedido Criado'
        WHEN CAB.STATUSNOTA = 'L' THEN 'Pedido Liberado'
        WHEN CAB.STATUSNOTA = 'A' THEN 'Pedido Aprovado'
        WHEN CAB.STATUSNOTA = 'F' THEN 'Pedido Faturado'
        WHEN CAB.STATUSNOTA = 'C' THEN 'Pedido Cancelado'
        ELSE 'Status Desconhecido'
    END AS activity,
    
    -- Timestamp da atividade
    COALESCE(CAB.DTFATUR, CAB.DTENTSAI, CAB.DTNEG) AS timestamp,
    
    -- Informações adicionais do processo
    CAB.CODEMP AS company_code,
    CAB.CODPARC AS partner_code,
    CAB.CODVEND AS seller_code,
    CAB.VLRNOTA AS order_value,
    TPO.DESCROPER AS operation_type,
    
    -- Recursos envolvidos
    CASE 
        WHEN CAB.CODVEND > 0 THEN 'Vendedor_' || CAB.CODVEND
        ELSE 'Sistema'
    END AS resource,
    
    -- Duração estimada (em minutos) - baseada em regras de negócio típicas
    CASE 
        WHEN CAB.STATUSNOTA = 'P' THEN 0  -- Criação é instantânea
        WHEN CAB.STATUSNOTA = 'L' THEN 30 -- Liberação leva ~30 min
        WHEN CAB.STATUSNOTA = 'A' THEN 60 -- Aprovação leva ~1 hora
        WHEN CAB.STATUSNOTA = 'F' THEN 45 -- Faturamento leva ~45 min
        WHEN CAB.STATUSNOTA = 'C' THEN 15 -- Cancelamento leva ~15 min
        ELSE 30
    END AS estimated_duration_minutes,
    
    -- Categoria do processo
    CASE 
        WHEN TPO.TIPMOV = 'V' THEN 'Vendas'
        WHEN TPO.TIPMOV = 'C' THEN 'Compras'
        WHEN TPO.TIPMOV = 'D' THEN 'Devolução'
        ELSE 'Outros'
    END AS process_category

FROM TGFCAB CAB
INNER JOIN TGFTOP TPO ON CAB.CODTIPOPER = TPO.CODTIPOPER 
                      AND CAB.DHTIPOPER = TPO.DHALTER
WHERE 
    -- Filtrar apenas registros dos últimos 12 meses para performance
    CAB.DTNEG >= ADD_MONTHS(SYSDATE, -12)
    -- Filtrar apenas operações de venda para o piloto inicial
    AND TPO.TIPMOV = 'V'
    -- Excluir registros com status inválidos
    AND CAB.STATUSNOTA IS NOT NULL

UNION ALL

-- Eventos de itens de pedido (para análise mais granular)
SELECT 
    ITE.NUNOTA AS case_id,
    'Item Processado - ' || PRO.DESCRPROD AS activity,
    COALESCE(CAB.DTFATUR, CAB.DTENTSAI, CAB.DTNEG) AS timestamp,
    CAB.CODEMP AS company_code,
    CAB.CODPARC AS partner_code,
    CAB.CODVEND AS seller_code,
    ITE.VLRTOT AS order_value,
    TPO.DESCROPER AS operation_type,
    'Sistema' AS resource,
    10 AS estimated_duration_minutes, -- Processamento de item leva ~10 min
    'Processamento_Item' AS process_category

FROM TGFITE ITE
INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
INNER JOIN TGFTOP TPO ON CAB.CODTIPOPER = TPO.CODTIPOPER 
                      AND CAB.DHTIPOPER = TPO.DHALTER
INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
WHERE 
    CAB.DTNEG >= ADD_MONTHS(SYSDATE, -12)
    AND TPO.TIPMOV = 'V'
    AND CAB.STATUSNOTA IS NOT NULL
    -- Limitar a produtos com valor significativo para reduzir ruído
    AND ITE.VLRTOT > 100

ORDER BY case_id, timestamp;

-- =====================================================
-- COMENTÁRIOS SOBRE A VIEW
-- =====================================================

/*
ESTRUTURA DA VIEW:
- case_id: Identificador único do processo (NUNOTA)
- activity: Nome da atividade executada
- timestamp: Data/hora da execução da atividade
- company_code: Código da empresa
- partner_code: Código do parceiro/cliente
- seller_code: Código do vendedor
- order_value: Valor do pedido/item
- operation_type: Tipo de operação
- resource: Recurso que executou a atividade
- estimated_duration_minutes: Duração estimada da atividade
- process_category: Categoria do processo

TABELAS UTILIZADAS:
- TGFCAB: Cabeçalho das notas (pedidos)
- TGFTOP: Tipos de operação (configurações)
- TGFITE: Itens das notas
- TGFPRO: Produtos

FILTROS APLICADOS:
- Últimos 12 meses de dados
- Apenas operações de venda (TIPMOV = 'V')
- Status de nota válidos
- Itens com valor > 100 (para reduzir ruído)

POSSÍVEIS EXTENSÕES:
1. Adicionar eventos de aprovação de crédito
2. Incluir eventos de logística/entrega
3. Adicionar eventos financeiros (pagamentos)
4. Incluir eventos de estoque
5. Adicionar eventos de workflow personalizado

PERFORMANCE:
- Índices recomendados:
  * TGFCAB(DTNEG, STATUSNOTA)
  * TGFCAB(CODTIPOPER, DHTIPOPER)
  * TGFITE(NUNOTA, VLRTOT)
*/

-- =====================================================
-- SCRIPT DE TESTE DA VIEW
-- =====================================================

-- Teste básico da view
SELECT COUNT(*) as total_events FROM VW_PROCESS_MINING_EVENTS;

-- Verificar distribuição de atividades
SELECT activity, COUNT(*) as frequency 
FROM VW_PROCESS_MINING_EVENTS 
GROUP BY activity 
ORDER BY frequency DESC;

-- Verificar casos com mais atividades
SELECT case_id, COUNT(*) as num_activities 
FROM VW_PROCESS_MINING_EVENTS 
GROUP BY case_id 
ORDER BY num_activities DESC 
FETCH FIRST 10 ROWS ONLY;

-- Verificar período de dados
SELECT 
    MIN(timestamp) as earliest_event,
    MAX(timestamp) as latest_event,
    COUNT(DISTINCT case_id) as unique_cases
FROM VW_PROCESS_MINING_EVENTS;

