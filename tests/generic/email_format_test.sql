{% test email_format(model, column_name) %}

select *
from {{ model }}
where not {{ column_name }} rlike '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

{% endtest %}
