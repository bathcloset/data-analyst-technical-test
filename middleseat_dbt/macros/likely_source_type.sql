--dbt setup to templatize a repeptive task
--for segementing actblue donations into origin buckets aka donation came from email or donation came from the website 
--if there is a source type listed we use that and if not the code below is to group and standarize partial info into buckets 
--case whens in SQL to format and group info coming in from ActBlue sync to create a dashboard with a labeled dropdown 
{% macro likely_source_type(source_type, refcode=none, form_name=none) -%}
{% set search_fields = [refcode, form_name] %}

    CASE 
        WHEN {{ source_type }} IS NOT NULL THEN {{ source_type }}

        {% for field in search_fields %}
            WHEN LEFT(lower(replace( {{ field }},'_','-')), 2) = 'em' THEN 'Email'
            WHEN LEFT(lower(replace( {{ field }},'_','-')), 3) = 'ads' THEN 'Ads'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%p2p%' AND lower(replace( {{ field }},'_','-')) ilike '%-rental-%' THEN 'Texting - P2P Rental'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%p2p%' THEN 'Texting - Owned P2P'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%sms%' AND NOT lower(replace( {{ field }},'_','-')) ilike '%p2p%' THEN 'Texting - Broadcast'
            WHEN lower(replace( {{ field }},'_','-')) ilike 'social' THEN 'Social'
            WHEN lower(replace( {{ field }},'_','-')) ilike '%web%' THEN 'Website'
        {% endfor %}
        
        WHEN lower({{ form_name }}) = 'actblue express donor dashboard contribution' THEN 'ActBlue Donor Dashboard'
        ELSE NULL
        END

{%- endmacro %}
