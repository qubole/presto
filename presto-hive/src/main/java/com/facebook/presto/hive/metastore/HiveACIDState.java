/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.ConnectorSession;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class HiveACIDState
{
    private Optional<Long> txnId = Optional.empty();
    private AtomicBoolean txnValidAtMetastore = new AtomicBoolean(true);
    private ScheduledFuture heartBeatTask;
    private String validWriteIds;
    private Duration heartbeatInterval;

    private static final Logger log = Logger.get(HiveACIDState.class);

    public HiveACIDState(Duration heartbeatInterval)
    {
        this.heartbeatInterval = heartbeatInterval;
    }

    public HiveACIDState setTxnId(long txnId, ConnectorSession session)
    {
        if (isTxnOpen()) {
            throw new IllegalArgumentException("Setting another hive transaction while one is already open: " + this.txnId.get());
        }
        this.txnId = Optional.of(txnId);

        return this;
    }

    public HiveACIDState setHeartBeatTask(ScheduledExecutorService heartBeater, ExtendedHiveMetastore metastore)
    {
        checkArgument(isTxnOpen(), "Hive transaction should be open before starting the Heartbeat");

        heartBeatTask = heartBeater.scheduleWithFixedDelay(
            new HeartbeatRunnable(txnId.get(), metastore, txnValidAtMetastore),
                (long) (heartbeatInterval.toMillis() * 0.75 * Math.random()),
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        return this;
    }

    public HiveACIDState setValidWriteIds(String validWriteIds)
    {
        this.validWriteIds = validWriteIds;
        return this;
    }

    public String getValidWriteIds()
    {
        return validWriteIds;
    }

    public boolean isTxnOpen()
    {
        return txnId.isPresent();
    }

    public long getTxnId()
    {
        checkArgument(isTxnOpen(), "Hive transaction not open");
        return txnId.get();
    }

    public void endHeartBeat(ConnectorSession session)
    {
        log.info("Terminated heartbeat thread for query: " + session.getQueryId());
        heartBeatTask.cancel(true);
    }

    public boolean isTrasactionValidAtMetastore()
    {
        return txnValidAtMetastore.get();
    }

    private static class HeartbeatRunnable
            implements Runnable
    {
        private final long txnId;
        private final ExtendedHiveMetastore metastore;
        private final AtomicBoolean valid;

        public HeartbeatRunnable(long txnId, ExtendedHiveMetastore metastore, AtomicBoolean valid)
        {
            this.txnId = txnId;
            this.metastore = metastore;
            this.valid = valid;
        }

        @Override
        public void run()
        {
            if (valid.get()) {
                // TODO: Ideally, if txn invalid then we should cancel the query asap but right now wait for cleanupQuery to check validity
                valid.set(metastore.sendTxnHeartBeatAndFindIfValid(txnId));

                if (!valid.get()) {
                    log.error("Txn HeartBeat: Transaction aborted by Metastore: " + txnId);
                }
            }
        }
    }
}
