//go:build darwin && cgo

#import <Foundation/Foundation.h>
#include <sys/stat.h>
#include <stdlib.h>
#include "icloud_darwin.h"

int materialize_icloud_file(const char *path, char **errMsg) {
    @autoreleasepool {
        NSString *nsPath = [NSString stringWithUTF8String:path];
        NSURL *url = [NSURL fileURLWithPath:nsPath];
        NSError *error = nil;

        BOOL ok = [[NSFileManager defaultManager]
            startDownloadingUbiquitousItemAtURL:url
            error:&error];
        if (!ok && error != nil) {
            const char *desc = [[error localizedDescription] UTF8String];
            *errMsg = strdup(desc);
            return 1;
        }
        return 0;
    }
}

int evict_icloud_file(const char *path, char **errMsg) {
    @autoreleasepool {
        NSString *nsPath = [NSString stringWithUTF8String:path];
        NSURL *url = [NSURL fileURLWithPath:nsPath];
        NSError *error = nil;

        BOOL ok = [[NSFileManager defaultManager]
            evictUbiquitousItemAtURL:url
            error:&error];
        if (!ok && error != nil) {
            const char *desc = [[error localizedDescription] UTF8String];
            *errMsg = strdup(desc);
            return 1;
        }
        return 0;
    }
}

int is_icloud_evicted(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return 0;
    }
    // SF_DATALESS = 0x40000000 — kernel marks evicted iCloud files with this flag
    return (st.st_flags & 0x40000000) != 0 ? 1 : 0;
}
