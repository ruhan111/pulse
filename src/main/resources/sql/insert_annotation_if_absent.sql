-- Records one fact about an event unless it is already known. The first record wins, so
-- annotated_at stays the time the source first reported it. 1 affected row = new, 0 = known.
insert into event_annotations (event_id, kind, annotated_at)
values (:eventId, :kind, :annotatedAt)
on conflict (event_id, kind) do nothing
