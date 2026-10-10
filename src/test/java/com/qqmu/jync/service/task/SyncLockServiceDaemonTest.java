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
}
