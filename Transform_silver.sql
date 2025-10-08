-- Silver transformation in SQL

-- Create table
CREATE TABLE IF NOT EXISTS gic.public.ext_funds_silver (
    FINANCIAL_TYPE VARCHAR(255) NOT NULL,
    SYMBOL VARCHAR(50) NOT NULL,
    SECURITY_NAME VARCHAR(255) NOT NULL,
    SEDOL VARCHAR(255),
    PRICE FLOAT,
    QUANTITY FLOAT,
    REALISED_PL FLOAT,
    MARKET_VALUE FLOAT NOT NULL,
    FILENAME VARCHAR(255) NOT NULL,
    REPORTG_DATE DATE,        -- for date part from filename
    PORTFOLIO VARCHAR(255)        -- for rest of filename 
);

-- Insert into table
INSERT INTO gic.public.ext_funds_silver (
    FINANCIAL_TYPE, SYMBOL, SECURITY_NAME, SEDOL, PRICE, QUANTITY, REALISED_PL, MARKET_VALUE, FILENAME,
    PORTFOLIO, REPORTG_DATE
)
SELECT 
    FINANCIAL_TYPE, SYMBOL, SECURITY_NAME, SEDOL, PRICE, QUANTITY, REALISED_PL, MARKET_VALUE, FILENAME,
    TRIM(
        REGEXP_REPLACE(
            split_part(FILENAME, '.', 1), -- Reference the column defined in Step1
            '^(Fund |Report-of-|TT_monthly_|rpt-|mend-report )',
            '',
            'i' -- 'i' for case-insensitive
        ),
        ' -' -- Trim any remaining leading/trailing spaces or hyphens
    ) AS Portfolio,
	CASE
        -- Format: YYYY-MM-DD
        WHEN filename ~ '[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN
            TO_DATE(
                REGEXP_REPLACE(filename, '.*?([0-9]{4}-[0-9]{2}-[0-9]{2}).*', '\1'),
                'YYYY-MM-DD'
            )
                    -- Format: 8 digits (YYYYMMDD)
        WHEN filename ~ '[0-9]{8}' THEN
            TO_DATE(
                REGEXP_REPLACE(filename, '.*?([0-9]{8}).*', '\1'),
                'YYYYMMDD'
            )
         WHEN filename ~ '[0-9]{2}[-_][0-9]{2}[-_][0-9]{4}' then
         TO_DATE(
                REGEXP_REPLACE(filename, '.*?([0-9]{2})[-_]([0-9]{2})[-_]([0-9]{4}).*$', '\1-\2-\3'),
                case
                	when cast(SPLIT_PART(
                		REGEXP_REPLACE(filename, '.*?([0-9]{2})[-_]([0-9]{2})[-_]([0-9]{4}).*$', '\1-\2-\3'),'-',1)
                		as INT) > 12 
                		then 'DD-MM-YYYY'
                	else 'MM-DD-YYYY'
                end
                )
        ELSE NULL
    END AS reportg_date
FROM gic.public.ext_funds_raw;
