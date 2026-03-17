#ifndef ICLOUD_DARWIN_H
#define ICLOUD_DARWIN_H

// materialize_icloud_file triggers iCloud download for the given path.
// Returns 0 on success, non-zero on failure. Sets errMsg on failure.
int materialize_icloud_file(const char *path, char **errMsg);

// evict_icloud_file triggers iCloud eviction for the given path.
// Returns 0 on success, non-zero on failure. Sets errMsg on failure.
int evict_icloud_file(const char *path, char **errMsg);

// is_icloud_evicted checks if a file is an iCloud dataless stub.
// Returns 1 if evicted, 0 otherwise.
int is_icloud_evicted(const char *path);

#endif
