package com.cex.matching.infrastructure.wal;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.matching.application.service.RecordedMatchingCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileMatchingCommandJournalTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void append_shouldPersistCommandsAndReadOnlyAfterSnapshotSequence() {
        FileMatchingCommandJournal journal = new FileMatchingCommandJournal(
                new ObjectMapper().findAndRegisterModules(), temporaryDirectory.toString());
        journal.append("BTC_USDT", 1L, event("event-1", "1"));
        journal.append("BTC_USDT", 2L, event("event-2", "2"));

        List<RecordedMatchingCommand> commands = journal.readAfter("BTC_USDT", 1L);

        assertThat(commands).hasSize(1);
        assertThat(commands.get(0).sequence()).isEqualTo(2L);
        assertThat(commands.get(0).event().getEventId()).isEqualTo("event-2");
    }

    @Test
    void compact_shouldRemoveCommandsCoveredBySnapshot() {
        FileMatchingCommandJournal journal = new FileMatchingCommandJournal(
                new ObjectMapper().findAndRegisterModules(), temporaryDirectory.toString());
        journal.append("BTC_USDT", 1L, event("event-1", "1"));
        journal.append("BTC_USDT", 2L, event("event-2", "2"));

        journal.compact("BTC_USDT", 1L);

        List<RecordedMatchingCommand> commands = journal.readAfter("BTC_USDT", 0L);
        assertThat(commands).hasSize(1);
        assertThat(commands.get(0).sequence()).isEqualTo(2L);
    }

    private OrderEvent event(String eventId, String orderId) {
        return OrderEvent.builder().eventId(eventId).orderId(orderId).userId(7L).symbol("BTC_USDT")
                .action(OrderEvent.Action.SUBMIT).side(OrderEvent.OrderSide.BUY)
                .type(OrderEvent.OrderType.LIMIT).price(new BigDecimal("100")).quantity(BigDecimal.ONE)
                .timeInForce(OrderEvent.TimeInForce.GTC).timestamp(1L).build();
    }
}
