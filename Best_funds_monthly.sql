/*
4. Based on your understanding of the data, use Python and SQL to generate analysis which gives the 
best performing fund (highest rate of return) for every month. Programmatically generate the analysis as a CSV file.

    You can use the formula 
    Rate of Return = (Fund_MV_end - Fund_MV_start + Realized P/L) / Fund_MV_start
    Fund Market Value = Sum of (Symbol Market Value) for all symbols in that fund.
    Fund Realized P/L = Sum of (Symbol Realized P/L) for the same monthly period.
*/

COPY (
with rank_funds as (
select 
sum(efs.market_value) as market_value,
sum(efs.realised_pl) as pl,
LAG(SUM(market_value)) OVER (ORDER BY portfolio, efs.reportg_date) AS prev_month_value,
(sum(efs.market_value)-LAG(SUM(market_value)) OVER (ORDER BY portfolio, efs.reportg_date)+sum(efs.realised_pl))/ LAG(SUM(market_value)) OVER (ORDER BY portfolio, efs.reportg_date)as rate_of_return,
efs.reportg_date ,
efs.portfolio 
from ext_funds_silver efs 
group by efs.reportg_date , efs.portfolio
order by portfolio, efs.reportg_date 
),
ranked as (
select 
rank() over (partition by reportg_date order by rate_of_return desc) as ranking,
round(rate_of_return::numeric, 4) as rate_of_return  ,
portfolio ,
reportg_date 
from rank_funds 
where 1=1 
and reportg_date > '2022-08-31'
)
select *
from ranked 
WHERE ranking = 1
ORDER BY reportg_date, portfolio
) TO '/Best_funds_monthly.csv' CSV HEADER;

