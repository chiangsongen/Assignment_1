/*
3. Use Python and SQL to generate a price reconciliation analysis that shows the difference between `the price on EOM date` 
from the reference data vs `the price` in the fund report. Programmatically generate the analysis as a CSV file. 
If the price is unavailable in reference db, use the last available price.
	a. show the break/difference with instrument date ref price vs fund price.
	b. consider how the solution can be scalable for `N` number of fund reports in future.
*/

COPY (
WITH latest_price_dates AS (
    SELECT
        ep."SYMBOL",
        MAX(to_date("DATETIME", 'MM/DD/YYYY')) AS latest_price_date,
        f.reportg_date,
        f.reportg_date - MAX(to_date("DATETIME", 'MM/DD/YYYY')) as date_diff
    FROM equity_prices ep 
    JOIN public.ext_funds_silver f ON f.symbol = ep."SYMBOL"
    WHERE to_date("DATETIME", 'MM/DD/YYYY') <= f.reportg_date   -- target_date is the date you want price for
    GROUP BY ep."SYMBOL", f.reportg_date
    union 
    SELECT
        bp."ISIN",
        MAX(to_date("DATETIME", 'YYYY-MM-DD')) AS latest_price_date,
        f.reportg_date,
        f.reportg_date - MAX(to_date("DATETIME", 'YYYY-MM-DD')) as date_diff
    FROM bond_prices bp  
    JOIN public.ext_funds_silver f ON f.symbol = bp."ISIN"
    WHERE to_date("DATETIME", 'YYYY-MM-DD') <= f.reportg_date   -- target_date is the date you want price for
    GROUP BY bp."ISIN", f.reportg_date
),
latest_price as(
	select 
	    ep."SYMBOL",
        to_date("DATETIME", 'MM/DD/YYYY') AS date,
        ep."PRICE" 
	from equity_prices ep 
	union all
	select 
		bp."ISIN",
        to_date("DATETIME", 'YYYY-MM-DD') AS date,
        bp."PRICE" 
	from bond_prices bp 
)
select
	f.portfolio ,
    f.symbol,
    f.reportg_date,
    f.price,
    lpd.latest_price_date,
    lp."PRICE" as latest_price,
    f.reportg_date - lpd.latest_price_date as date_diff,
    round((f.price - lp."PRICE")::numeric,2) as price_diff
FROM public.ext_funds_silver f
LEFT JOIN latest_price_dates lpd ON f.symbol = lpd."SYMBOL" 
	and f.reportg_date = lpd.reportg_date 
LEFT JOIN latest_price lp ON lpd."SYMBOL" = lp."SYMBOL"
	and lpd.latest_price_date = lp.date
where 1=1
	--and f.portfolio = 'Applebead'
	and round((f.price - lp."PRICE")::numeric,2) <> 0
order by f.portfolio , f.reportg_date, f.symbol, date_diff
) TO '/Price_diff.csv' CSV HEADER;
