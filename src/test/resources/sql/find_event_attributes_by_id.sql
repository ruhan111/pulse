select attribute.key, attribute.value
from events, jsonb_each_text(events.attributes) as attribute
where events.id = :id
