package com.cex.matching.infrastructure.wal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/** 使用固定字段顺序和 CRC32 校验 WAL 记录。 */
public final class WalCodec {
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonFactory factory = mapper.getFactory();

    public String encode(WalRecord record) {
        try {
            String payload = writePayload(record);
            CRC32 crc = new CRC32();
            crc.update(payload.getBytes(StandardCharsets.UTF_8));
            return writeComplete(record, crc.getValue());
        } catch (Exception e) {
            throw new WalCorruptionException("WAL 记录编码失败", e);
        }
    }

    public WalRecord decode(String line) {
        try {
            JsonNode node = mapper.readTree(line);
            if (node == null || !node.isObject()) {
                throw new WalCorruptionException("WAL JSON 格式不合法");
            }
            WalRecord record = new WalRecord(
                    requiredLong(node, "sequence"), requiredText(node, "commandId"),
                    requiredText(node, "orderId"), requiredText(node, "userId"),
                    requiredText(node, "symbol"), requiredEnum(node, "commandType", CommandType.class),
                    optionalEnum(node, "side", MatchOrder.Side.class), requiredLong(node, "price"),
                    requiredLong(node, "quantity"), requiredLong(node, "timestamp"), 0L);
            long supplied = requiredLong(node, "checksum");
            CRC32 crc = new CRC32();
            crc.update(writePayload(record).getBytes(StandardCharsets.UTF_8));
            if (supplied != crc.getValue()) {
                throw new WalCorruptionException("WAL 校验和不一致");
            }
            return record.withChecksum(supplied);
        } catch (WalCorruptionException e) {
            throw e;
        } catch (Exception e) {
            throw new WalCorruptionException("WAL JSON 解析失败", e);
        }
    }

    private String writePayload(WalRecord record) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (JsonGenerator generator = factory.createGenerator(output)) {
            writeFields(generator, record, true);
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    private String writeComplete(WalRecord record, long checksum) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (JsonGenerator generator = factory.createGenerator(output)) {
            writeFields(generator, record, false);
            generator.writeNumberField("checksum", checksum);
            generator.writeEndObject();
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    private void writeFields(JsonGenerator generator, WalRecord record, boolean endObject) throws Exception {
        generator.writeStartObject();
        generator.writeNumberField("sequence", record.sequence());
        generator.writeStringField("commandId", record.commandId());
        generator.writeStringField("orderId", record.orderId());
        generator.writeStringField("userId", record.userId());
        generator.writeStringField("symbol", record.symbol());
        generator.writeStringField("commandType", record.commandType().name());
        if (record.side() == null) {
            generator.writeNullField("side");
        } else {
            generator.writeStringField("side", record.side().name());
        }
        generator.writeNumberField("price", record.price());
        generator.writeNumberField("quantity", record.quantity());
        generator.writeNumberField("timestamp", record.timestamp());
        if (endObject) {
            generator.writeEndObject();
        }
    }

    private static String requiredText(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull() || !value.isTextual()) {
            throw new WalCorruptionException("WAL 缺少有效字段: " + name);
        }
        return value.textValue();
    }

    private static String optionalText(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new WalCorruptionException("WAL 字段类型不合法: " + name);
        }
        return value.textValue();
    }

    private static <E extends Enum<E>> E requiredEnum(JsonNode node, String name, Class<E> type) {
        return Enum.valueOf(type, requiredText(node, name));
    }

    private static <E extends Enum<E>> E optionalEnum(JsonNode node, String name, Class<E> type) {
        String value = optionalText(node, name);
        return value == null ? null : Enum.valueOf(type, value);
    }

    private static long requiredLong(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isIntegralNumber()) {
            throw new WalCorruptionException("WAL 缺少有效字段: " + name);
        }
        return value.longValue();
    }
}
