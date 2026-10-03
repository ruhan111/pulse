-- Inserts one event unless its id is already stored. The first version wins: a re-polled item
-- never overwrites ingested_at. Reports 1 affected row for a new event, 0 for a duplicate.
insert into events (id, source, channel, external_id, type, occurred_at, ingested_at, title, url, summary, attributes)
values (:id, :source, :channel, :externalId, :type, :occurredAt, :ingestedAt, :title, :url, :summary,
		jsonb_object(cast(:attributeKeys as text[]), cast(:attributeValues as text[])))
on conflict (id) do nothing
