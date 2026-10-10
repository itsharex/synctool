package com.qqmu.jync.service.task;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.qqmu.jync.model.CaptureStrategy;
import com.qqmu.jync.model.Project;
import com.qqmu.jync.repository.ProjectRepository;
import com.qqmu.jync.service.cdc.CdcConsumerManager;

/**
 * The periodic convergence each node runs against the shared database.
 *
 * <p>Presence only, in both directions: enabled-but-not-scheduled-here gets scheduled,
 * scheduled-here-but-disabled gets removed, and anything already in step is left alone —
 * a reconciler that re-created matching jobs every tick would reset trigger timings and
 * starve short poll intervals.
 */
@ExtendWith(MockitoExtension.class)
class ScheduleReconcilerTest {

    @Mock private ProjectRepository projectRepository;
    @Mock private SyncScheduler scheduler;
    @Mock private CdcConsumerManager cdcManager;

    private ScheduleReconciler reconciler;

    @BeforeEach
    void setUp() {
        reconciler = new ScheduleReconciler(projectRepository, scheduler, cdcManager);
    }

    private static Project project(long id, String name, boolean enabled) {
        Project p = new Project();
        p.setId(id);
        p.setName(name);
        p.setEnabled(enabled);
        return p;
    }

    @Test
    void aProjectStartedOnAnotherNodeGetsScheduledHere() {
        Project started = project(1L, "started-elsewhere", true);
        when(projectRepository.findAll()).thenReturn(List.of(started));
        when(scheduler.isScheduled(1L)).thenReturn(false);

        reconciler.reconcile();

        verify(scheduler).schedule(started);
        verify(scheduler, never()).unschedule(anyLong());
    }

    @Test
    void aProjectStoppedOnAnotherNodeGetsUnscheduledHere() {
        when(projectRepository.findAll()).thenReturn(List.of(project(2L, "stopped-elsewhere", false)));
        when(scheduler.isScheduled(2L)).thenReturn(true);

        reconciler.reconcile();

        verify(scheduler).unschedule(2L);
        verify(scheduler, never()).schedule(any());
    }

    @Test
    void schedulesAlreadyInStepAreLeftAlone() {
        when(projectRepository.findAll()).thenReturn(List.of(
                project(1L, "running", true),
                project(2L, "stopped", false)));
        when(scheduler.isScheduled(1L)).thenReturn(true);
        when(scheduler.isScheduled(2L)).thenReturn(false);

        reconciler.reconcile();

        verify(scheduler, never()).schedule(any());
        verify(scheduler, never()).unschedule(anyLong());
    }

    @Test
    void oneBrokenProjectDoesNotStopTheOthers() {
        Project bad = project(1L, "bad-cron", true);
        Project good = project(2L, "good", true);
        when(projectRepository.findAll()).thenReturn(List.of(bad, good));
        when(scheduler.isScheduled(1L)).thenReturn(false);
        when(scheduler.isScheduled(2L)).thenReturn(false);
        org.mockito.Mockito.doThrow(new IllegalStateException("bad cron"))
                .when(scheduler).schedule(bad);

        reconciler.reconcile();

        verify(scheduler).schedule(good);
    }

    @Test
    void aDatabaseHiccupIsSurvivedAndTheTickSimplyRetriesLater() {
        when(projectRepository.findAll()).thenThrow(new RuntimeException("db down"));

        assertThatCode(() -> reconciler.reconcile()).doesNotThrowAnyException();
        verify(scheduler, never()).schedule(any());
    }

    // —— CDC 分支(T13):BINLOG_CDC 项目归 CdcConsumerManager 管,永不进 Quartz ——

    @Test
    void anEnabledCdcProjectGetsAConsumerAndNoQuartzJob() {
        Project cdc = project(3L, "cdc-stream", true);
        when(projectRepository.findAll()).thenReturn(List.of(cdc));
        when(cdcManager.strategyOf(cdc)).thenReturn(CaptureStrategy.BINLOG_CDC);

        reconciler.reconcile();

        verify(scheduler).removePollingJob(3L);        // 幂等清掉可能残留的轮询 job
        verify(cdcManager).ensureStarted(cdc);         // 接管入口
        verify(scheduler, never()).schedule(any());    // 绝不建 Quartz job
        verify(cdcManager, never()).ensureStopped(anyLong());
    }

    @Test
    void aDisabledCdcProjectGetsItsConsumerStopped() {
        Project cdc = project(4L, "cdc-stopped", false);
        when(projectRepository.findAll()).thenReturn(List.of(cdc));
        when(cdcManager.strategyOf(cdc)).thenReturn(CaptureStrategy.BINLOG_CDC);

        reconciler.reconcile();

        verify(scheduler).removePollingJob(4L);
        verify(cdcManager).ensureStopped(4L);
        verify(cdcManager, never()).ensureStarted(any());
        verify(scheduler, never()).schedule(any());
    }
}
