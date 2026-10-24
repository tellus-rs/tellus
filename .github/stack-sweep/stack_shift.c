#define _GNU_SOURCE
#include <pthread.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
struct launch { void *(*f)(void *); void *arg; size_t shift; };
__attribute__((constructor)) static void announce(void) {
    const char *value = getenv("STACK_SHIFT");
    fprintf(stderr, "stack_shift=%lu\n", value ? strtoul(value, 0, 10) : 0);
}
__attribute__((noinline)) static void *entry(void *p) {
    struct launch launch = *(struct launch *)p;
    free(p);
    volatile char *pad = alloca(launch.shift + 16);
    pad[0] = 1;
    void *result = launch.f(launch.arg);
    pad[0] = 0;
    return result;
}
int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*f)(void *), void *arg) {
    int (*real_create)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *) =
        dlsym(RTLD_NEXT, "pthread_create");
    const char *value = getenv("STACK_SHIFT");
    struct launch *p = malloc(sizeof(*p));
    *p = (struct launch){ f, arg, value ? strtoul(value, 0, 10) : 0 };
    int result = real_create(thread, attr, entry, p);
    if (result) free(p);
    return result;
}
