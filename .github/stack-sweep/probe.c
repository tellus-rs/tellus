#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
__attribute__((noinline)) static void *offset(void *out) {
    volatile char local = 0;
    *(uintptr_t *)out = (uintptr_t)&local % 4096;
    return 0;
}
int main(void) {
    uintptr_t out;
    pthread_t thread;
    if (pthread_create(&thread, 0, offset, &out) || pthread_join(thread, 0)) return 1;
    printf("%lu\n", (unsigned long)out);
    return 0;
}
