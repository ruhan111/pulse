/**
 * Implementations of domain ports using concrete technology: PostgreSQL for
 * {@link com.example.pulse.event.EventSink} (Phase 3), and later possibly Kafka, Redis, etc.
 * <p>
 * Rules: may depend on every other package; no other package may depend on it. Dependencies point
 * inward, from technology to domain, so the technology can be swapped out.
 */
package com.example.pulse.infrastructure;
