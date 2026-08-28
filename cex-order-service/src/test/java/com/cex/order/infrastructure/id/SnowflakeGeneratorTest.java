package com.cex.order.infrastructure.id;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SnowflakeGeneratorTest {

    @Test
    void nextId_isUniqueAndIncreasing() {
        SnowflakeGenerator generator = new SnowflakeGenerator(1, 1);
        long prev = generator.nextId();
        for (int i = 0; i < 10_000; i++) {
            long next = generator.nextId();
            assertThat(next).isGreaterThan(prev);
            prev = next;
        }
    }

    @Test
    void nextId_concurrent_neverDuplicates() throws InterruptedException {
        SnowflakeGenerator generator = new SnowflakeGenerator(1, 1);
        int threads = 8;
        int perThread = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        Set<Long> ids = java.util.Collections.synchronizedSet(new HashSet<>());

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        ids.add(generator.nextId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(ids).hasSize(threads * perThread);
    }
}
