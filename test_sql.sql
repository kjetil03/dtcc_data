if object_id ('tempdb.dbo.#newcusips', 'U') is not null
drop table #newcusips;

if object_id ('tempdb.dbo.#maxmaturities', 'U') is not null
drop table #maxmaturities;


select cusip

into #newcusips 

FROM   [NBDataHub].[DTCC].[CPCDtransactions]
where settlementdate >= '2022-01-01'


SELECT a.[CUSIP]
,[MaturityDate]
,max(datediff(day, settlementdate, maturitydate)) as max_maturity
into #maxmaturities
FROM [NBDataHub].[DTCC].[CPCDtransactions] as a
where a.cusip in #newcusips.cusip 
group by a.cusip, MaturityDate