select
    id,
    name,
    description
from tests
where
    id = %(id)s;
