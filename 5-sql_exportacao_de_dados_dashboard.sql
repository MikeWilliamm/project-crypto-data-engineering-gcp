-- Tabela completa unindo todas as 4 análises do BigQuery
SELECT 
  -- Dados principais (daily_overview)
  overview.id,
  overview.name,
  overview.symbol,
  overview.rank,
  overview.price_usd,
  overview.market_cap_usd,
  overview.volume_usd_24hr,
  overview.change_percent_24hr,
  overview.vwap_24hr,
  overview.supply,
  overview.max_supply,
  overview.explorer,
  
  -- Dados de supply dynamics (campos únicos)
  supply.market_cap_per_unit_supply,
  supply.status_oferta_maxima,
  
  -- Dados de dominância de mercado (campo único)
  dominance.percent_market_cap,
  
  -- Dados de movimento (campo único)
  gainers.tipo_movimento,
  
  -- Data de referência
  overview.data_referencia

FROM `acoes-378306.coincap.daily_overview` overview

LEFT JOIN `acoes-378306.coincap.supply_dynamics` supply
  ON overview.symbol = supply.symbol
  AND overview.data_referencia = supply.data_referencia

LEFT JOIN `acoes-378306.coincap.market_dominance` dominance
  ON overview.symbol = dominance.symbol  
  AND overview.data_referencia = dominance.data_referencia

LEFT JOIN `acoes-378306.coincap.top_gainers_losers` gainers
  ON overview.symbol = gainers.symbol
  AND overview.data_referencia = gainers.data_referencia

-- Filtra pela data mais recente
WHERE overview.data_referencia = (
  SELECT MAX(data_referencia) 
  FROM `acoes-378306.coincap.daily_overview`
)

ORDER BY overview.rank ASC