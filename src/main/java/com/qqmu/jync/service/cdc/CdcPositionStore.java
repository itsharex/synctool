package com.qqmu.jync.service.cdc;

import java.time.Instant;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.qqmu.jync.model.CdcPosition;
import com.qqmu.jync.repository.CdcPositionRepository;

/**
 * Transactional gateway for {@link CdcPosition}, mirroring the SyncLockStore pattern:
 * a separate bean so REQUIRES_NEW actually proxies, letting the CDC consumer persist
 * positions from its own long-running thread without an ambient transaction.
 */
@Service
public class CdcPositionStore {

    private final CdcPositionRepository repository;

    public CdcPositionStore(CdcPositionRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Optional<CdcPosition> find(Long projectId) {
        return repository.findByProjectId(projectId);
    }

    /** Upsert the start-of-baseline position; baselineDone resets to false. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CdcPosition recordBaselineStart(Long projectId, String file, long pos,
                                           String gtidSet, long serverId) {
        CdcPosition position = repository.findByProjectId(projectId).orElseGet(() -> {
            CdcPosition created = new CdcPosition();
            created.setProjectId(projectId);
            return created;
        });
        position.setBinlogFile(file);
        position.setBinlogPos(pos);
        position.setGtidSet(gtidSet);
        position.setServerId(serverId);
        position.setBaselineDone(false);
        position.setUpdatedAt(Instant.now());
        return repository.save(position);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markBaselineDone(Long projectId) {
        repository.findByProjectId(projectId).ifPresent(position -> {
            position.setBaselineDone(true);
            position.setUpdatedAt(Instant.now());
            repository.save(position);
        });
    }

    /** Post-commit position advance. Silently ignored when the row vanished underneath us. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void advance(Long projectId, String file, long pos, String gtidSet) {
        repository.findByProjectId(projectId).ifPresent(position -> {
            position.setBinlogFile(file);
            position.setBinlogPos(pos);
            if (gtidSet != null) {
                position.setGtidSet(gtidSet);
            }
            position.setUpdatedAt(Instant.now());
            repository.save(position);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void delete(Long projectId) {
        repository.deleteByProjectId(projectId);
    }
}
