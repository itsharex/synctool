package com.qqmu.jync.service.cdc;

import lombok.Getter;

/** One persistable position: binlog file + next-position, plus the GTID set when available. */
@Getter
public final class CdcPositionMark {
    private final String file;
    private final long pos;
    private final String gtidSet;

    public CdcPositionMark(String file, long pos, String gtidSet) {
        this.file = file;
        this.pos = pos;
        this.gtidSet = gtidSet;
    }
}
