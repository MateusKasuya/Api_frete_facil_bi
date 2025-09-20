DROP VIEW VWCPTIT_BI;
CREATE VIEW VWCPTIT_BI AS
SELECT
        c.nrocontrole,
        c.anocontrole,
        c.cgccpfforne AS codfornecedor,
        f.nomefantasia AS fornecedor,
        c.codtransacao,
        h.descricao AS transacao,
        c.vlrtotal - c.vlrsaldo AS vlrpago,
        c.vlrsaldo,
        c.datamovto,
        c.datavencto,
        EXTRACT(DAY FROM c.datamovto) dia_movto,
		EXTRACT(MONTH FROM c.datamovto) mes_numero_movto,
		EXTRACT(YEAR FROM c.datamovto) ano_movto,
        EXTRACT(DAY FROM c.datavencto) dia_vencto,
		EXTRACT(MONTH FROM c.datavencto) mes_numero_vencto,
		EXTRACT(YEAR FROM c.datavencto) ano_vencto,
        CASE
            WHEN c.vlrsaldo = 0 THEN 'Pago'
            WHEN c.vlrsaldo > 0 AND CURRENT_TIMESTAMP <= CAST(c.datavencto AS TIMESTAMP) THEN 'A Pagar'
            ELSE 'Em Atraso'
        END AS condicao_fatura,
        c.contareduz,
        cta.nomeconta AS conta
    FROM
        cptit c 
    LEFT JOIN tbfor f ON f.cgccpfforne  = c.cgccpfforne
    LEFT JOIN tbhis h ON h.codtransacao = c.codtransacao
    LEFT JOIN tbcta cta ON cta.contareduz = c.contareduz