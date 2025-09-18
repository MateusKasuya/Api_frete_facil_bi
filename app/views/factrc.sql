DROP VIEW FACTRC_BI;
CREATE VIEW FACTRC_BI AS
SELECT
	f.nrofatura,
	f.anofatura,
	c.nomefantasia cliente,
	f.cgccpffatura codcliente,
	t.nome filial,
	f.codfilfatur codfilial,
	f.codciddes AS codcid,
	cid.cidade,
	cid.coduf,
	cid.regiao,
	f.codpro,
	p.nome AS produto,
	f.dataemissao,
	f.datavencto,
	f.datarecbto,
		CASE
		WHEN EXTRACT(MONTH FROM f.datarecbto) = 1 THEN 'Janeiro'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 2 THEN 'Fevereiro'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 3 THEN 'Março'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 4 THEN 'Abril'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 5 THEN 'Maio'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 6 THEN 'Junho'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 7 THEN 'Julho'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 8 THEN 'Agosto'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 9 THEN 'Setembro'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 10 THEN 'Outubro'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 11 THEN 'Novembro'
		WHEN EXTRACT(MONTH FROM f.datarecbto)= 12 THEN 'Dezembro'
	END AS mes_recbto,
	EXTRACT(MONTH FROM f.datarecbto) mes_numero,
	EXTRACT(YEAR FROM f.datarecbto) ano_recbto,
	EXTRACT(DAY FROM f.datarecbto) dia_recbto,
	f.datarecbto - f.dataemissao AS dias_recebimento,
	f.vlrfatura,
	f.vlrrecbto,
	f.vlrsaldo,
	CASE
		WHEN CAST(f.datavencto AS TIMESTAMP) < CURRENT_TIMESTAMP
		AND f.vlrsaldo > 0 THEN 'Em Atraso'
		WHEN f.vlrsaldo = 0 THEN 'Recebida'
		ELSE 'A Receber'
	END AS condicao_fatura,
	f.contareduz,
	f.codtransacao
FROM
	factrc f
LEFT JOIN tbfil t ON t.codfil = f.codfilfatur
LEFT JOIN tbcli c ON c.cgccpfcli = f.cgccpffatura
LEFT JOIN tbcid_bi cid ON cid.codcid = f.codcidpagto
LEFT JOIN tbpro p ON p.codpro = f.codpro