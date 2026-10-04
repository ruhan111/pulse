-- Replaces a stream's position. Unlike events, the latest version wins: only where to resume matters.
insert into checkpoints (stream, position, updated_at)
values (:stream, :position, :updatedAt)
on conflict (stream) do update set position = excluded.position, updated_at = excluded.updated_at
