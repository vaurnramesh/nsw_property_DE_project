{% macro classify_property_flag(section_no_col, street_no_col, property_category_col) %}
    CASE
        WHEN UPPER(TRIM({{ adapter.quote(property_category_col) }})) = 'V'
             AND {{ adapter.quote(section_no_col) }} IS NOT NULL
             THEN 'UNIT_WITH_VACANTLAND_FLAG'
        WHEN UPPER(TRIM({{ adapter.quote(property_category_col) }})) = 'V'
             AND REGEXP_CONTAINS(
                 UPPER(TRIM({{ adapter.quote(street_no_col) }})),
                 r'^\d+\s?[A-Z]'
             )
             THEN 'TOWNHOUSE_WITH_VACANTLAND_FLAG'
        WHEN {{ adapter.quote(section_no_col) }} IS NOT NULL 
             AND REGEXP_CONTAINS(
                 UPPER(TRIM({{ adapter.quote(street_no_col) }})),
                 r'^\d+\s?[A-Z]'
             )
             THEN 'TOWNHOUSE_WITH_UNIT_FLAG'
        ELSE NULL
    END
{% endmacro %}
