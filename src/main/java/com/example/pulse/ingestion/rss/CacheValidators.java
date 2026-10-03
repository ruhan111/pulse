package com.example.pulse.ingestion.rss;

/**
 * What a server told us about the version of a feed we last fetched. Sent back on the next request
 * so the server can answer {@code 304 Not Modified} instead of the whole feed.
 *
 * @param etag         the {@code ETag} response header, or empty
 * @param lastModified the {@code Last-Modified} response header, or empty
 */
record CacheValidators(String etag, String lastModified) {

	public static final CacheValidators NONE = new CacheValidators("", "");

	public CacheValidators {
		etag = etag == null ? "" : etag;
		lastModified = lastModified == null ? "" : lastModified;
	}

}
