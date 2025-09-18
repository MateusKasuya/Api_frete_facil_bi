CREATE VIEW VWTBCID_BI AS
SELECT
	codcid,
	nome AS cidade,
	coduf,
	nome || '-' || coduf AS cidade_uf,
	CASE
		WHEN coduf = 'SP' THEN 'Sudeste'
		WHEN coduf = 'RJ' THEN 'Sudeste'
		WHEN coduf = 'MG' THEN 'Sudeste'
		WHEN coduf = 'ES' THEN 'Sudeste'
		WHEN coduf = 'PR' THEN 'Sul'
		WHEN coduf = 'SC' THEN 'Sul'
		WHEN coduf = 'RS' THEN 'Sul'
		WHEN coduf = 'MS' THEN 'Centro-Oeste'
		WHEN coduf = 'MT' THEN 'Centro-Oeste'
		WHEN coduf = 'GO' THEN 'Centro-Oeste'
		WHEN coduf = 'DF' THEN 'Centro-Oeste'
		WHEN coduf = 'RO' THEN 'Norte'
		WHEN coduf = 'AC' THEN 'Norte'
		WHEN coduf = 'AM' THEN 'Norte'
		WHEN coduf = 'RR' THEN 'Norte'
		WHEN coduf = 'PA' THEN 'Norte'
		WHEN coduf = 'TO' THEN 'Norte'
		WHEN coduf = 'AP' THEN 'Norte'
		WHEN coduf = 'MA' THEN 'Nordeste'
		WHEN coduf = 'PI' THEN 'Nordeste'
		WHEN coduf = 'CE' THEN 'Nordeste'
		WHEN coduf = 'RN' THEN 'Nordeste'
		WHEN coduf = 'PB' THEN 'Nordeste'
		WHEN coduf = 'PE' THEN 'Nordeste'
		WHEN coduf = 'AL' THEN 'Nordeste'
		WHEN coduf = 'SE' THEN 'Nordeste'
		WHEN coduf = 'BA' THEN 'Nordeste'
		WHEN coduf = 'TO' THEN 'Norte'
		WHEN coduf = 'AP' THEN 'Norte'
		WHEN coduf = 'MA' THEN 'Nordeste'
		WHEN coduf = 'PI' THEN 'Nordeste'
		WHEN coduf = 'CE' THEN 'Nordeste'
		WHEN coduf = 'RN' THEN 'Nordeste'
		WHEN coduf = 'PB' THEN 'Nordeste'
		WHEN coduf = 'PE' THEN 'Nordeste'
		WHEN coduf = 'AL' THEN 'Nordeste'
		WHEN coduf = 'SE' THEN 'Nordeste'
		WHEN coduf = 'BA' THEN 'Nordeste'
		ELSE 'Outros'
	END AS regiao
FROM
	tbcid
