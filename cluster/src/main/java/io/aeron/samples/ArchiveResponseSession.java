/*
 * Copyright 2023 Adaptive Financial Consulting
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.samples;

import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.archive.client.AeronArchive;
import io.aeron.samples.cluster.ClusterConfig;

/**
 * A standing archive client whose control responses arrive on an Aeron response channel
 * ({@code control-mode=response}).
 * <p>
 * Lucern's test harness uses this so a long-lived response channel exists in a real driver: the
 * request publication, the response subscription and the archive's response publication (carrying
 * {@code response-correlation-id}) are all present at steady state, in the cluster node's own driver.
 * <p>
 * It is a separate client because none of the cluster's own archive clients can do this: the
 * consensus module, clustered service container and cluster backup all reject a local archive
 * control channel that is not IPC.
 * <p>
 * The archive pings each control session over its response publication and closes the session once
 * a ping cannot be sent, so the response subscription is drained on a daemon thread for as long as
 * the session is open.
 */
public final class ArchiveResponseSession implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveResponseSession.class);

    /**
     * Offset from base port for the response channel's control address. Follows
     * {@link ClusterConfig#TRANSFER_PORT_OFFSET}, the last offset ClusterConfig itself allocates.
     */
    private static final int ARCHIVE_RESPONSE_PORT_OFFSET = ClusterConfig.TRANSFER_PORT_OFFSET + 1;

    private static final long POLL_INTERVAL_MS = 100;

    private final AeronArchive archive;
    private final Thread poller;
    private volatile boolean running = true;

    private ArchiveResponseSession(final AeronArchive archive)
    {
        this.archive = archive;
        this.poller = new Thread(this::pollUntilClosed, "archive-response-keepalive");
        this.poller.setDaemon(true);
    }

    /**
     * Connect to this node's archive over its UDP control channel, with responses on a response
     * channel, and start draining the responses.
     *
     * @param clusterConfig this node's cluster configuration; its archive must already be running
     * @param nodeId        this member's id
     * @param hostname      this member's hostname
     * @param portBase      the cluster port base
     * @return the open session
     */
    public static ArchiveResponseSession open(
        final ClusterConfig clusterConfig, final int nodeId, final String hostname, final int portBase)
    {
        final int responsePort = ClusterConfig.calculatePort(nodeId, portBase, ARCHIVE_RESPONSE_PORT_OFFSET);
        final AeronArchive.Context ctx = new AeronArchive.Context()
            .aeronDirectoryName(clusterConfig.mediaDriverContext().aeronDirectoryName())
            .controlRequestChannel(clusterConfig.archiveContext().controlChannel())
            .controlRequestStreamId(clusterConfig.archiveContext().controlStreamId())
            .controlResponseChannel("aeron:udp?control-mode=response|control=" + hostname + ":" + responsePort);

        final ArchiveResponseSession session = new ArchiveResponseSession(AeronArchive.connect(ctx));
        session.poller.start();
        LOGGER.info("Archive response channel session open: controlSessionId={} responseChannel={}",
            session.archive.controlSessionId(), ctx.controlResponseChannel());
        return session;
    }

    private void pollUntilClosed()
    {
        try
        {
            while (running)
            {
                final String error = archive.pollForErrorResponse();
                if (null != error)
                {
                    LOGGER.warn("Archive response channel session error: {}", error);
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
        catch (final RuntimeException ex)
        {
            if (running)
            {
                LOGGER.warn("Archive response channel session stopped polling", ex);
            }
        }
    }

    /**
     * Stop draining responses and close the archive client, which also closes its Aeron client.
     */
    public void close()
    {
        running = false;
        poller.interrupt();
        try
        {
            poller.join();
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
        CloseHelper.quietClose(archive);
    }
}
