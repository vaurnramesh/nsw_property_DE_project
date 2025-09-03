{% macro classify_property_type(section_no_col, street_no_col, property_category_col) %}
    CASE
        WHEN UPPER(TRIM({{ adapter.quote(property_category_col) }})) = 'V' THEN 'Vacant Land'
        WHEN {{ adapter.quote(section_no_col) }} IS NOT NULL THEN 'UNIT/APARTMENT'
        WHEN REGEXP_CONTAINS(
                UPPER(TRIM({{ adapter.quote(street_no_col) }})),
                r'^\d+\s?[A-Z]'
             ) THEN 'Townhouse'
        ELSE 'HOUSE'
    END
{% endmacro %}
