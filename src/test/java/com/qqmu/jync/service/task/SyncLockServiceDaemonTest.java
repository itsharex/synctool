package com.qqmu.jync.service.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.qqmu.jync.config.SyncProperties;

/**
 * 守护租约(CDC 消费者)必须能续过轮询路径的 10 次看门狗上限,否则长驻流每
 * ttl×10 必然掉锁一次,双实例环境里表现为周期性无主抖动(B6)。
 * 搭法照抄 SyncLockServiceRenewTest:mock SyncLockStore + new SyncLockService(store, props, 8080)。
 * 上限本身用 maxWatchdogRenewals() 数值钉死(等 11 个看门狗周期要 11 秒,不值);
 * 定时器真的在续租用 ttl=3s(间隔 1s)的快路径验证。
 */
class SyncLockServiceDaemonTest {

    private SyncLockStore store;

    private SyncLockService service() {
        store = mock(SyncLockStore.class);
        when(store.acquire(eq(9L), anyString(), anyLong())).thenReturn(true);
        when(store.renew(eq(9L), anyString(), anyLong())).thenReturn(true);
        return new SyncLockService(store, new SyncProperties(), 8080);
    }

    @Test
    void daemonLeaseHasUnboundedWatchdogRenewals() {
        SyncLockService.LockHandle handle = service().tryAcquireForDaemon(9L, 30_000L);
        assertThat(handle).isNotNull();
        assertThat(handle.maxWatchdogRenewals()).isEqualTo(Integer.MAX_VALUE);
        handle.close();
    }

    @Test
    void pollingLeaseKeepsTenRenewalCap() {
        SyncLockService.LockHandle handle = service().tryAcquire(9L);
        assertThat(handle).isNotNull();
        assertThat(handle.maxWatchdogRenewals()).isEqualTo(10);
        handle.close();
    }

    @Test
    void daemonWatchdogRenewsOnTimer() {
        // ttl=3000 → 看门狗间隔 max(1000, 3000/3)=1000ms,3 秒内至少 2 次续租
        SyncLockService.LockHandle handle = service().tryAcquireForDaemon(9L, 3_000L);
        verify(store, timeout(3_500).atLeast(2)).renew(eq(9L), anyString(), anyLong());
        handle.close();
    }

    // —— FIX A(评审 Critical):跨线程 close 必须真正释放 JVM 锁 ——
    // 守护租约在 manager 调用者线程 acquire(ensureStarted),在消费者线程 close(CdcConsumer
    // finally,brief 钉死)。ReentrantLock 的 unlock 只在持有线程生效(isHeldByCurrentThread
    // 守卫)→ 跨线程 close 后 JVM 锁永久泄漏:本节点对该项目再也无法重启/重建基线/回退
    // 轮询/删除,直到进程重启。进程内互斥必须线程无关(Semaphore(1))。

    @Test
    void crossThreadCloseReleasesJvmLock() throws Exception {
        SyncLockService service = service();
        java.util.concurrent.atomic.AtomicReference<SyncLockService.LockHandle> ref =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread acquirer = new Thread(() -> ref.set(service.tryAcquireForDaemon(9L, 30_000L)));
        acquirer.start();
        acquirer.join(5_000);
        assertThat(ref.get()).isNotNull();                       // 线程 A 拿到租约

        Thread closer = new Thread(() -> ref.get().close());     // 线程 B close(消费者线程形态)
        closer.start();
        closer.join(5_000);

        SyncLockService.LockHandle again = service.tryAcquireForDaemon(9L, 30_000L);
        assertThat(again).isNotNull();                           // JVM 锁已释放:可再次获取
        again.close();
    }

    // —— 终审 FIX 1:rebaseline holder 门的只读租约查询 ——
    // 必须复用 acquireLock 的同一条 TTL 新鲜度语义(lockExpiresAt < now = 过期 = 可接管),
    // 不许发明第二种「live owner」:外来且未过期,是唯一读作「他节点持有」的形态。

    private static com.qqmu.jync.model.SyncTask lease(String owner, java.time.Instant expiresAt) {
        com.qqmu.jync.model.SyncTask task = new com.qqmu.jync.model.SyncTask();
        task.setProjectId(9L);
        task.setLockOwner(owner);
        task.setLockExpiresAt(expiresAt);
        return task;
    }

    @Test
    void foreignFreshLeaseReadsAsHeldByAnotherLiveOwner() {
        SyncLockService service = service();
        // 新鲜的外来租约 = 持有者的看门狗还在续 → 正是 gate 必须拒绝的形态
        when(store.findTask(9L)).thenReturn(java.util.Optional.of(
                lease("other-host:8080:deadbeef", java.time.Instant.now().plusSeconds(30))));
        assertThat(service.isLeaseHeldByAnotherLiveOwner(9L)).isTrue();

        // 自己的租约永远不算「他节点持有」(acquireLock 的 lockOwner = :owner 分支)
        when(store.findTask(9L)).thenReturn(java.util.Optional.of(
                lease(service.getOwnerId(), java.time.Instant.now().plusSeconds(30))));
        assertThat(service.isLeaseHeldByAnotherLiveOwner(9L)).isFalse();
    }

    @Test
    void expiredOrAbsentLeaseReadsAsTakeableExactlyLikeAcquire() {
        SyncLockService service = service();
        // 过期外来租约 = acquireLock 眼里可接管(lockExpiresAt < now)→ gate 必须放行
        // (恢复路径:死节点/致命 park 释放后的重建基线)
        when(store.findTask(9L)).thenReturn(java.util.Optional.of(
                lease("other-host:8080:deadbeef", java.time.Instant.now().minusSeconds(1))));
        assertThat(service.isLeaseHeldByAnotherLiveOwner(9L)).isFalse();

        // 无任务行/租约已释放(lockOwner null 形态)→ 放行
        when(store.findTask(9L)).thenReturn(java.util.Optional.of(
                lease(null, null)));
        assertThat(service.isLeaseHeldByAnotherLiveOwner(9L)).isFalse();
        when(store.findTask(9L)).thenReturn(java.util.Optional.empty());
        assertThat(service.isLeaseHeldByAnotherLiveOwner(9L)).isFalse();
    }

    @Test
    void doubleCloseDoesNotCorruptExclusion() throws Exception {
        // close 必须恰好释放一次:Semaphore 二次 release 会把许可加到 2 → 互斥被吹掉
        // (两个「持有者」并发写同一项目)。AtomicBoolean CAS 守卫钉死幂等。
        SyncLockService service = service();
        SyncLockService.LockHandle handle = service.tryAcquireForDaemon(9L, 30_000L);
        assertThat(handle).isNotNull();
        Thread first = new Thread(handle::close);
        first.start();
        first.join(5_000);
        Thread second = new Thread(handle::close);               // 竞态重复 close
        second.start();
        second.join(5_000);

        SyncLockService.LockHandle fresh = service.tryAcquireForDaemon(9L, 30_000L);
        assertThat(fresh).isNotNull();                           // 恰好一次释放 → 可重新获取
        assertThat(service.tryAcquireForDaemon(9L, 30_000L)).isNull(); // 许可未膨胀 → 互斥仍成立
        fresh.close();
    }
}
