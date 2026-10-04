-- The saved position of one stream; no row if it has none.
select position from checkpoints where stream = :stream
