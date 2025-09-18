DROP VIEW VWFRCTRC_BI;
CREATE VIEW VWFRCTRC_BI AS
SELECT
	f.nroctrc,
	f.ufctrc,
	f.dataemissao,
	CASE
		WHEN EXTRACT(MONTH FROM f.dataemissao) = 1 THEN 'Janeiro'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 2 THEN 'Fevereiro'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 3 THEN 'Março'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 4 THEN 'Abril'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 5 THEN 'Maio'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 6 THEN 'Junho'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 7 THEN 'Julho'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 8 THEN 'Agosto'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 9 THEN 'Setembro'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 10 THEN 'Outubro'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 11 THEN 'Novembro'
		WHEN EXTRACT(MONTH FROM f.dataemissao)= 12 THEN 'Dezembro'
	END AS mes_emissao,
	EXTRACT(YEAR FROM f.dataemissao) ano_emissao,
	EXTRACT(MONTH FROM f.dataemissao) mes_numero,
	EXTRACT(DAY FROM f.dataemissao) dia_emissao,
	f.situacao,
	f.indctetpcte,
	t.nome filial,
	f.codfilemite codfilial,
	c.nomefantasia cliente,
	f.cgccpfdestina codcliente,
	f.codciddes AS codcid,
	cid.regiao,
	cid.cidade,
	cid.coduf,
	f.codpro,
	f.totalpeso / 1000.0 AS pesofrete_ton,
	f.vlrpedagio,
	f.vlrcarreto + f.vlrseguromerca + f.vlrpis + f.vlriapas + f.vlricmsst + f.vlricms AS vlrcusto,
	CASE
		WHEN f.indctetpcte = 0 THEN 1
		ELSE 0
	END AS embarque,
	CASE
		WHEN f.situacao = 'F' THEN 1
		ELSE 0
	END AS faturado
FROM
	frctrc f
LEFT JOIN tbfil t ON t.codfil = f.codfilemite
LEFT JOIN tbcli c ON f.cgccpfdestina = c.cgccpfcli
LEFT JOIN vwtbcid_bi cid ON cid.codcid = f.codciddes
WHERE
	f.situacao IN ('N', 'F')
