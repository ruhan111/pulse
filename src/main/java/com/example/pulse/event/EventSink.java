package com.example.pulse.event;

/**
 * Where ingested events go. Source adapters publish into this port and never know what is behind it.
 * <p>
 * Phase 3 implements it with PostgreSQL. If load testing shows ingestion and processing need to be
 * decoupled, a Kafka-backed implementation can replace it without touching any adapter.
 * <p>
 * Contract: implementations must be idempotent on {@link PulseEvent#id()}. Accepting the same
 * event twice has the same effect as accepting it once.
 */
@FunctionalInterface
public interface EventSink {

	void accept(PulseEvent event);

}
