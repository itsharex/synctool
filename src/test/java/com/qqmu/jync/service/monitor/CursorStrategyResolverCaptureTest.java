package com.qqmu.jync.service.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import com.qqmu.jync.config.SyncProperties;
import com.qqmu.jync.dto.SyncConfig;
import com.qqmu.jync.dto.meta.ColumnMeta;
import com.qqmu.jync.dto.meta.TableMeta;
import com.qqmu.jync.model.CaptureStrategy;

class CursorStrategyResolverCaptureTest {

    private final CursorStrategyResolver resolver = new CursorStrategyResolver(new SyncProperties());

    private static TableMeta tableWithUpdatedAt() {
        TableMeta table = new TableMeta();
        table.setName("orders");
        ColumnMeta id = new ColumnMeta();
        id.setName("id");
        id.setJdbcType(Types.BIGINT);
        id.setPrimaryKey(true);
        ColumnMeta updatedAt = new ColumnMeta();
        updatedAt.setName("updated_at");
        updatedAt.setJdbcType(Types.TIMESTAMP);
        table.getColumns().add(id);
        table.getColumns().add(updatedAt);
        table.getPrimaryKeys().add("id");
        return table;
    }

    @Test
    void pollingKeepsAutoResolution() {
        SyncConfig config = new SyncConfig();
        config.setCaptureStrategy(CaptureStrategy.POLLING);
        CursorStrategy strategy = resolver.resolve(tableWithUpdatedAt(), config);
        assertThat(strategy.getKind()).isEqualTo(CursorStrategy.Kind.TIMESTAMP);
    }

    @Test
    void nullStrategyKeepsAutoResolution() {
        CursorStrategy strategy = resolver.resolve(tableWithUpdatedAt(), new SyncConfig());
        assertThat(strategy.getKind()).isEqualTo(CursorStrategy.Kind.TIMESTAMP);
    }

    @Test
    void fullCompareOverrideForcesFullCompare() {
        SyncConfig config = new SyncConfig();
        config.setCaptureStrategy(CaptureStrategy.FULL_COMPARE);
        CursorStrategy strategy = resolver.resolve(tableWithUpdatedAt(), config);
        assertThat(strategy.getKind()).isEqualTo(CursorStrategy.Kind.FULL_COMPARE);
    }
}
