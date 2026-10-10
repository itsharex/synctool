package com.qqmu.jync.service.cdc;

import org.springframework.stereotype.Component;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.qqmu.jync.config.SyncProperties;
import com.qqmu.jync.model.CdcPosition;
import com.qqmu.jync.model.DatabaseConfig;
import com.qqmu.jync.util.CryptoUtil;

/**
 * Assembles the {@link BinaryLogClient} for a CDC project (task book B1).
 *
 * <p>The Debezium fork keeps the com.github.shyiko package names and authenticates with
 * caching_sha2_password, so MySQL 8.4+ sources (which removed mysql_native_password)
 * work out of the box.
 *
 * <p>Deserializer compatibility modes pin the wire format for the row mapper:
 * <ul>
 *   <li>{@code DATE_AND_TIME_AS_LONG_MICRO} — temporal cells arrive as epoch-micros
 *       Longs; no java.util.Date timezone guesswork.</li>
 *   <li>{@code INVALID_DATE_AND_TIME_AS_MIN_VALUE} — zero dates arrive as
 *       Long.MIN_VALUE, which the mapper turns into null + a warn counter.</li>
 *   <li>{@code CHAR_AND_BINARY_AS_BYTE_ARRAY} — char/binary cells arrive as raw
 *       bytes; the mapper decodes them with the column's own charset.</li>
 * </ul>
 *
 * <p>keepAlive is disabled on purpose: reconnect policy (exponential backoff, fatal
 * classification) lives entirely in {@link CdcConsumer} so there is exactly one place
 * that decides when to retry.
 */
@Component
public class BinlogClientFactory {

    private final CryptoUtil cryptoUtil;
    private final SyncProperties properties;

    public BinlogClientFactory(CryptoUtil cryptoUtil, SyncProperties properties) {
        this.cryptoUtil = cryptoUtil;
        this.properties = properties;
    }

    /** The fake replica id this project presents: offset + projectId, never colliding internally. */
    public long serverIdFor(long projectId) {
        return properties.getCdc().getServerIdOffset() + projectId;
    }

    public BinaryLogClient createClient(DatabaseConfig source, long projectId, CdcPosition resumeFrom) {
        String host = source.getHost() == null || source.getHost().trim().isEmpty()
                ? "localhost" : source.getHost().trim();
        int port = source.getPort() != null ? source.getPort() : source.getType().getDefaultPort();
        BinaryLogClient client = new BinaryLogClient(host, port,
                source.getUsername(), cryptoUtil.decrypt(source.getPassword()));
        client.setServerId(serverIdFor(projectId));
        client.setKeepAlive(false);
        client.setEventDeserializer(deserializer());
        if (resumeFrom != null) {
            if (resumeFrom.getGtidSet() != null && !resumeFrom.getGtidSet().trim().isEmpty()) {
                client.setGtidSet(resumeFrom.getGtidSet().trim());
            } else {
                client.setBinlogFilename(resumeFrom.getBinlogFile());
                client.setBinlogPosition(resumeFrom.getBinlogPos() == null ? 4L : resumeFrom.getBinlogPos());
            }
        }
        return client;
    }

    private static EventDeserializer deserializer() {
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
                EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        return deserializer;
    }
}
