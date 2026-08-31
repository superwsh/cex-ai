package com.cex.matching.infrastructure.kafka;

import com.cex.matching.application.service.MatchingSnapshotService;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MatchingConsumerRebalanceListenerTest {

    @Test
    void onPartitionsRevoked_shouldPersistAllActiveOrderBookSnapshots() {
        MatchingSnapshotService snapshotService = mock(MatchingSnapshotService.class);
        MatchingConsumerRebalanceListener listener = new MatchingConsumerRebalanceListener(snapshotService);

        listener.onPartitionsRevoked(List.of(new TopicPartition("cex.order.created", 1)));

        verify(snapshotService).saveAllSnapshots();
    }
}
