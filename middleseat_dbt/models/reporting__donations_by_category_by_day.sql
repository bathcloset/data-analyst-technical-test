{{
    config(
        dist='wdl_client_code',
        sort=['wdl_client_code', 'likely_source_type']
    )
}}

with base as (
    SELECT
    wdl_client_code,
    --date info 
    CAST(et_created_at AS DATE) AS  et_created_at,
    et_created_year,
    et_created_month,
    row_number() over (partition by lower(email) order by date(DATETIME(et_created_date, "America/New_York")) asc) as first_donation,
    --name info 
    COALESCE(likely_source_type, 'None') AS likely_source_type,
    form_managing_entity_committee_name,
    committee_name,
    COALESCE(recurring_type, 'None') AS recurring,
    SUM(post_refund_amount) AS dollars_raised,
    COUNT(DISTINCT wdl_transaction_id) AS number_of_donations,
    --donor indo
    COUNT(DISTINCT email) AS unique_donors,
    COUNT(DISTINCT CASE WHEN is_recurring THEN wdl_transaction_id END) AS recurring_donations
FROM {{ ref('core__donations')}}
    WHERE is_recurring_cancelled is NULL
    )
,report_view as (
    SELECT 
        et_created_at,
        et_created_year,
        et_created_month,
        dollars_raised,
        likely_source_type,
        recurring, 
        number_of_donations, 
        unique_donors,
        recurring_donations
    FROM base
GROUP BY wdl_client_code, et_created_date, likely_source_type, form_managing_entity_committee_name, committee_name, recurring
ORDER BY wdl_client_code, et_created_date DESC, likely_source_type
    )
