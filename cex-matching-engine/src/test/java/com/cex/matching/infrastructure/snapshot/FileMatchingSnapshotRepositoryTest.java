package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.TimeInForce;
import com.cex.matching.domain.model.OrderBookSnapshot;
import com.cex.matching.domain.model.ProcessedMatchResultSnapshot;
import com.cex.matching.domain.model.RestingOrderSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileMatchingSnapshotRepositoryTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void save_shouldAtomicallyReplaceAndLoadLatestSnapshot() {
        FileMatchingSnapshotRepository repository = new FileMatchingSnapshotRepository(
                new ObjectMapper().findAndRegisterModules(), temporaryDirectory.toString());
        OrderBookSnapshot first = snapshot(1L, "0.8");
        OrderBookSnapshot latest = snapshot(2L, "0.6");

        repository.save(first);
        repository.save(latest);

        OrderBookSnapshot loaded = repository.load("BTC_USDT").orElseThrow();
        assertThat(loaded.sequence()).isEqualTo(2L);
        assertThat(loaded.orders().get(0).remainingQuantity()).isEqualByComparingTo("0.6");
        assertThat(loaded.processedResults()).singleElement()
                .extracting(ProcessedMatchResultSnapshot::eventId).isEqualTo("event-2");
    }

    private OrderBookSnapshot snapshot(long sequence, String remainingQuantity) {
        return new OrderBookSnapshot("BTC_USDT", sequence, 0L, List.of(new RestingOrderSnapshot(
                1L, 7L, "BTC_USDT", OrderSide.SELL, new BigDecimal("100"), BigDecimal.ONE,
                new BigDecimal(remainingQuantity), TimeInForce.GTC, Instant.EPOCH, 1L)),
                List.of(new ProcessedMatchResultSnapshot("event-" + sequence, sequence, OrderStatus.OPEN,
                        new BigDecimal(remainingQuantity), null, List.of())));
    }
}
