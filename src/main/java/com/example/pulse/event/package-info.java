/**
 * The core domain: what a {@link com.example.pulse.event.PulseEvent} is, where it came from, and the
 * {@link com.example.pulse.event.EventSink} port that events are published into.
 * <p>
 * Rules: this package depends on nothing but the JDK. No Spring, no JPA, no HTTP. Every other
 * package may depend on it; it depends on none of them. Enforced by {@code ArchitectureTest}.
 */
package com.example.pulse.event;
