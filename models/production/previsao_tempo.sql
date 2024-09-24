WITH WeatherData AS (
    SELECT
        SUBSTRING(Coordenada, CHARINDEX('Lat ', Coordenada) + 4, 6) AS Lat,
        SUBSTRING(Coordenada, CHARINDEX('Lon ', Coordenada) + 4, 6) AS Lon,
        [Data],
        [Temperatura (°C)],
        [Velocidade do Vento (km/h)],
        [Precipitação (mm)]
    FROM  {{ref('weather_api')}} 
),
coord as (
	select 
		[Local],
		CAST(cb.Lat AS DECIMAL(6,2)) as Lat,
		abs(CAST(cb.Lon AS DECIMAL(6,2))) as Lon
	FROM {{source('challenge_raw', 'coordenadas_bairros')}} cb
),
previsao as (
	SELECT 
		wd.lat,
		wd.lon,
	    wd.[Data],
	    wd.[Temperatura (°C)] as Temperatura_C,
	    wd.[Velocidade do Vento (km/h)] as Velocidade_Vento_Km,
	    wd.[Precipitação (mm)] as Precipitacao_mm,
	    UPPER(cb.Local)  AS Bairro
	FROM WeatherData wd
	JOIN coord cb
	ON CAST(wd.Lat AS DECIMAL(6,2)) = cb.Lat
	AND CAST(wd.Lon AS DECIMAL(6,2)) = cb.Lon),
limiar_pluviometria as (
	select
		bairro,
		0.75 as Limiar_0_75,
		0.9 as Limiar_0_9
	from {{ref('limiar_pluviometria')}}
)
SELECT 
    p.lat,
    p.lon,
    p.Data,
    p.Temperatura_C,
    p.Velocidade_Vento_Km,
    p.Precipitacao_mm,
    p.Bairro,
    CASE 
        WHEN p.Precipitacao_mm < l.Limiar_0_75 THEN 'Normal'
        WHEN p.Precipitacao_mm >= l.Limiar_0_75 AND p.Precipitacao_mm < l.Limiar_0_9 THEN 'Médio'
        ELSE 'Alto'
    END AS nivel_alerta
FROM 
    previsao p
JOIN 
    limiar_pluviometria l
ON 
    p.Bairro = l.Bairro;