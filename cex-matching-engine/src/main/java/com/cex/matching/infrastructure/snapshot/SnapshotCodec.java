package com.cex.matching.infrastructure.snapshot;

import com.cex.matching.domain.snapshot.MatchingSnapshot;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 使用严格 JSON 格式编解码订单簿快照。 */
public final class SnapshotCodec {
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    /** @param snapshot 待编码快照 @return UTF-8 JSON 字节 */
    public byte[] encode(MatchingSnapshot snapshot) { try { return mapper.writeValueAsBytes(snapshot); } catch (Exception e) { throw new SnapshotException("快照编码失败", e); } }
    /** @param bytes 待解码 JSON 字节 @return 已校验快照 */
    public MatchingSnapshot decode(byte[] bytes) { try { return mapper.readValue(bytes, MatchingSnapshot.class); } catch (Exception e) { throw new SnapshotException("快照解码失败", e); } }
}
