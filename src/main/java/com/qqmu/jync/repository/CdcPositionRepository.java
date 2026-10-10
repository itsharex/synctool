package com.qqmu.jync.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.qqmu.jync.model.CdcPosition;

public interface CdcPositionRepository extends JpaRepository<CdcPosition, Long> {

    Optional<CdcPosition> findByProjectId(Long projectId);

    @Modifying
    @Query("delete from CdcPosition p where p.projectId = :projectId")
    void deleteByProjectId(@Param("projectId") Long projectId);
}
