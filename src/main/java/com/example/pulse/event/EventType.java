package com.example.pulse.event;

/**
 * What happened, independent of where it happened.
 * <p>
 * Deliberately small: a type earns its place when some part of Pulse needs to treat it
 * differently (e.g. trend detection weighting an edit differently from a new post).
 */
public enum EventType {

	/** Something new appeared: an article, a post, a story, a repository. */
	PUBLISHED,

	/** Something existing changed: a Wikipedia edit, an updated feed item. */
	EDITED,

	/** A reaction to something existing: a comment, a reply. */
	COMMENTED

}
