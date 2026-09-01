package com.cex.market.domain.model;

import com.cex.common.kafka.event.market.OrderBookDeltaEvent;
import com.cex.common.kafka.event.market.PriceLevelChange;
import com.cex.market.domain.exception.MarketSequenceGapException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 行情聚合订单簿对增量、顺序和重复投递的单元测试。 */
class MarketOrderBookTest {

    private static final String SYMBOL = "BTC_USDT";

    @Test
    void shouldApplyContinuousDeltaAndKeepPricePriority() {
        MarketOrderBook book = activeBook();

        DeltaApplyResult result = book.applyDelta(delta("delta-101", 101L, 100L,
                List.of(level("100.00", "3"), level("99.00", "2")),
                List.of(level("101.00", "4"), level("102.00", "5"))));

        assertThat(result).isEqualTo(DeltaApplyResult.APPLIED);
        assertThat(book.getSequence()).isEqualTo(101L);
        assertThat(book.getStatus()).isEqualTo(MarketDataStatus.ACTIVE);
        assertThat(book.bidDepth(10)).extracting(MarketPriceLevel::price)
                .containsExactly(new BigDecimal("100.00"), new BigDecimal("99.00"));
        assertThat(book.askDepth(10)).extracting(MarketPriceLevel::price)
                .containsExactly(new BigDecimal("101.00"), new BigDecimal("102.00"));
    }

    @Test
    void shouldRemovePriceLevelWhenQuantityIsZero() {
        MarketOrderBook book = activeBook();
        book.applyDelta(delta("delta-101", 101L, 100L, List.of(level("100", "2")), List.of()));

        book.applyDelta(delta("delta-102", 102L, 101L, List.of(level("100", "0")), List.of()));

        assertThat(book.bidDepth(10)).isEmpty();
        assertThat(book.getSequence()).isEqualTo(102L);
    }

    @Test
    void shouldReturnBestBidAndBestAskFromSortedPriceTrees() {
        MarketOrderBook book = activeBook();
        book.applyDelta(delta("delta-101", 101L, 100L,
                List.of(level("99", "1"), level("100", "2")),
                List.of(level("102", "3"), level("101", "4"))));

        assertThat(book.bestBid()).contains(marketLevel("100", "2"));
        assertThat(book.bestAsk()).contains(marketLevel("101", "4"));
    }

    @Test
    void shouldCreateImmutableDepthSnapshotAtCurrentSequence() {
        MarketOrderBook book = activeBook();
        book.applyDelta(delta("delta-101", 101L, 100L,
                List.of(level("100", "2"), level("99", "3")),
                List.of(level("101", "4"), level("102", "5"))));

        MarketDepthSnapshot snapshot = book.depthSnapshot(1, 1_700_000_000_100L);
        book.applyDelta(delta("delta-102", 102L, 101L, List.of(level("100", "0")), List.of()));

        assertThat(snapshot.symbol()).isEqualTo(SYMBOL);
        assertThat(snapshot.sequence()).isEqualTo(101L);
        assertThat(snapshot.timestamp()).isEqualTo(1_700_000_000_100L);
        assertThat(snapshot.bids()).containsExactly(marketLevel("100", "2"));
        assertThat(snapshot.asks()).containsExactly(marketLevel("101", "4"));
        assertThat(snapshot.bids()).isUnmodifiable();
    }

    @Test
    void shouldIgnoreDuplicateDeltaWithoutMutatingBook() {
        MarketOrderBook book = activeBook();
        OrderBookDeltaEvent event = delta("delta-101", 101L, 100L, List.of(level("100", "2")), List.of());
        book.applyDelta(event);

        DeltaApplyResult result = book.applyDelta(event);

        assertThat(result).isEqualTo(DeltaApplyResult.IGNORED_DUPLICATE);
        assertThat(book.getSequence()).isEqualTo(101L);
        assertThat(book.bidDepth(10)).containsExactly(marketLevel("100", "2"));
    }

    @Test
    void shouldInvalidateBookWhenSequenceHasGap() {
        MarketOrderBook book = activeBook();

        assertThatThrownBy(() -> book.applyDelta(delta("delta-102", 102L, 101L, List.of(), List.of())))
                .isInstanceOf(MarketSequenceGapException.class)
                .hasMessageContaining("localSequence=100")
                .hasMessageContaining("incomingPreviousSequence=101");
        assertThat(book.getStatus()).isEqualTo(MarketDataStatus.INVALID);
        assertThat(book.getSequence()).isEqualTo(100L);
    }

    @Test
    void shouldRejectDeltaBeforeSnapshotIsLoaded() {
        MarketOrderBook book = new MarketOrderBook(SYMBOL);

        assertThatThrownBy(() -> book.applyDelta(delta("delta-1", 1L, 0L, List.of(), List.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("INIT");
    }

    @Test
    void shouldRejectInvalidPriceLevel() {
        MarketOrderBook book = activeBook();

        assertThatIllegalArgumentException().isThrownBy(() -> book.applyDelta(
                delta("delta-101", 101L, 100L, List.of(level("100", "-1")), List.of())));
    }

    private MarketOrderBook activeBook() {
        MarketOrderBook book = new MarketOrderBook(SYMBOL);
        book.loadSnapshot(100L, List.of(), List.of());
        return book;
    }

    private OrderBookDeltaEvent delta(String eventId, long sequence, long previousSequence,
                                      List<PriceLevelChange> bids, List<PriceLevelChange> asks) {
        return OrderBookDeltaEvent.builder().eventId(eventId).eventVersion(1).symbol(SYMBOL)
                .sequence(sequence).previousSequence(previousSequence).bids(bids).asks(asks)
                .eventTime(1_700_000_000_000L).build();
    }

    private PriceLevelChange level(String price, String quantity) {
        return PriceLevelChange.builder().price(new BigDecimal(price)).quantity(new BigDecimal(quantity)).build();
    }

    private MarketPriceLevel marketLevel(String price, String quantity) {
        return new MarketPriceLevel(new BigDecimal(price), new BigDecimal(quantity));
    }
}
