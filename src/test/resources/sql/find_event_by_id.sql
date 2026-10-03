select source, channel, external_id, type, occurred_at, ingested_at, title, url, summary
from events
where id = :id
