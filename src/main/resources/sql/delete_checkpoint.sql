-- Forgets a stream's position, so the next connection starts from now.
delete from checkpoints where stream = :stream
