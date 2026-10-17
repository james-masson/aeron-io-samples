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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import io.aeron.samples.cluster.protocol.AddAuctionBidCommandEncoder;
import io.aeron.samples.cluster.protocol.AddAuctionBidCommandResultDecoder;
import io.aeron.samples.cluster.protocol.AddAuctionBidResult;
import io.aeron.samples.cluster.protocol.AddAuctionResult;
import io.aeron.samples.cluster.protocol.AddParticipantCommandEncoder;
import io.aeron.samples.cluster.protocol.AuctionListDecoder;
import io.aeron.samples.cluster.protocol.AuctionStatus;
import io.aeron.samples.cluster.protocol.CreateAuctionCommandEncoder;
import io.aeron.samples.cluster.protocol.CreateAuctionCommandResultDecoder;
import io.aeron.samples.cluster.protocol.ListAuctionsCommandEncoder;
import io.aeron.samples.cluster.protocol.MessageHeaderDecoder;
import io.aeron.samples.cluster.protocol.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple Aeron cluster load client. Connects to the cluster using the existing C media driver
 * (no embedded Java driver). When PUBLISH_RATE > 0 it sends AddAuctionBidCommand messages at
 * the target rate; each bid triggers an AuctionUpdated broadcast to all connected sessions,
 * so a consumer (PUBLISH_RATE=0) will see received counts climb.
 *
 * <p>On publisher startup a ListAuctions command is sent. If auction 1 already exists and is
 * OPEN, setup is skipped. If no auction exists, participants 1 (creator) and 2 (bidder) are
 * registered, then auction 1 is created. A CLOSED auction triggers an unclean exit so the pod
 * restarts and the cluster can be redeployed with fresh state.
 *
 * <p>Env vars:
 * <ul>
 *   <li>CLUSTER_ADDRESSES  — comma-separated cluster node hostnames (default: localhost)</li>
 *   <li>CLUSTER_PORT_BASE  — cluster port base (default: 9000)</li>
 *   <li>PUBLISH_RATE       — target messages/sec; 0 = consumer only (default: 0)</li>
 * </ul>
 * JVM property:
 * <ul>
 *   <li>aeron.dir — path to the existing media driver directory (default: /dev/shm/aeron)</li>
 * </ul>
 */
public class ClusterLoadClient implements EgressListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterLoadClient.class);
    private static final int MAX_CONSECUTIVE_BID_FAILURES = 10;
    private static final long CONNECT_DEADLINE_MS = 120_000;
    private static final long CONNECT_RETRY_PAUSE_MS = 500;

    private final AtomicLong received = new AtomicLong();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final AuctionListDecoder auctionListDecoder = new AuctionListDecoder();
    private final CreateAuctionCommandResultDecoder createAuctionResultDecoder =
        new CreateAuctionCommandResultDecoder();
    private final AddAuctionBidCommandResultDecoder addBidResultDecoder =
        new AddAuctionBidCommandResultDecoder();

    private volatile boolean auctionExists = false;
    private volatile AddAuctionResult auctionCreationResult = null;
    private volatile AuctionStatus foundAuctionStatus = null;
    private volatile long foundAuctionStartTime = 0L;
    private volatile long foundAuctionCurrentPrice = 0L;
    private int consecutiveBidFailures = 0;

    @Override
    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        received.incrementAndGet();
        headerDecoder.wrap(buffer, offset);
        switch (headerDecoder.templateId())
        {
            case AuctionListDecoder.TEMPLATE_ID ->
            {
                auctionListDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                final AuctionListDecoder.AuctionsDecoder entries = auctionListDecoder.auctions();
                while (entries.hasNext())
                {
                    entries.next();
                    if (entries.auctionId() == 1L)
                    {
                        auctionExists = true;
                        foundAuctionStatus = entries.status();
                        foundAuctionStartTime = entries.startTime();
                        foundAuctionCurrentPrice = entries.currentPrice();
                        break;
                    }
                }
            }
            case CreateAuctionCommandResultDecoder.TEMPLATE_ID ->
            {
                createAuctionResultDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                auctionCreationResult = createAuctionResultDecoder.result();
            }
            case AddAuctionBidCommandResultDecoder.TEMPLATE_ID ->
            {
                addBidResultDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                if (addBidResultDecoder.result() == AddAuctionBidResult.SUCCESS)
                {
                    consecutiveBidFailures = 0;
                }
                else
                {
                    consecutiveBidFailures++;
                }
            }
        }
    }

    /**
     * Entry point.
     * @param args unused
     */
    public static void main(final String[] args)
    {
        final String rawAddresses = getenv("CLUSTER_ADDRESSES", "localhost");
        final int portBase = Integer.parseInt(getenv("CLUSTER_PORT_BASE", "9000"));
        final int publishRate = Integer.parseInt(getenv("PUBLISH_RATE", "0"));
        final String aeronDir = System.getProperty("aeron.dir", "/dev/shm/aeron");

        final List<String> hostnames = Arrays.asList(rawAddresses.split(","));
        final String ingressEndpoints = ClusterConfig.ingressEndpoints(
            hostnames, portBase, ClusterConfig.CLIENT_FACING_PORT_OFFSET);

        LOGGER.info("Connecting: endpoints={} aeronDir={} publishRate={}", ingressEndpoints, aeronDir, publishRate);

        final String podIp = getenv("POD_IP", "0.0.0.0");
        final ClusterLoadClient client = new ClusterLoadClient();
        final AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try (AeronCluster aeronCluster = connectWithRetry(client, podIp, ingressEndpoints, aeronDir, running))
        {
            LOGGER.info("Connected to cluster.");

            final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
            final UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

            long auctionStartTime = System.currentTimeMillis();

            if (publishRate > 0)
            {
                // Query existing auctions — skip setup if auction 1 is already live
                final ListAuctionsCommandEncoder listCmd = new ListAuctionsCommandEncoder();
                listCmd.wrapAndApplyHeader(sendBuffer, 0, headerEncoder);
                listCmd.correlationId("setup-list");
                retryOffer(aeronCluster, sendBuffer,
                    MessageHeaderEncoder.ENCODED_LENGTH + listCmd.encodedLength(), running);

                final long listDeadline = System.currentTimeMillis() + 2_000;
                while (!client.auctionExists && System.currentTimeMillis() < listDeadline)
                {
                    aeronCluster.pollEgress();
                    Thread.yield();
                }

                if (!client.auctionExists)
                {
                    LOGGER.info("No existing auction — running setup.");

                    final AddParticipantCommandEncoder creatorEncoder = new AddParticipantCommandEncoder();
                    creatorEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder);
                    creatorEncoder.participantId(1L);
                    creatorEncoder.correlationId("setup-creator");
                    creatorEncoder.name("load-creator");
                    retryOffer(aeronCluster, sendBuffer,
                        MessageHeaderEncoder.ENCODED_LENGTH + creatorEncoder.encodedLength(), running);

                    final AddParticipantCommandEncoder bidderEncoder = new AddParticipantCommandEncoder();
                    bidderEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder);
                    bidderEncoder.participantId(2L);
                    bidderEncoder.correlationId("setup-bidder");
                    bidderEncoder.name("load-bidder");
                    retryOffer(aeronCluster, sendBuffer,
                        MessageHeaderEncoder.ENCODED_LENGTH + bidderEncoder.encodedLength(), running);

                    aeronCluster.pollEgress();
                    Thread.sleep(500);
                    aeronCluster.pollEgress();

                    auctionStartTime = System.currentTimeMillis() + 1_000;
                    final CreateAuctionCommandEncoder auctionEncoder = new CreateAuctionCommandEncoder();
                    auctionEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder);
                    auctionEncoder.createdByParticipantId(1L);
                    auctionEncoder.startTime(auctionStartTime);
                    auctionEncoder.endTime(auctionStartTime + 3_600_000L);
                    auctionEncoder.correlationId("setup-auction");
                    auctionEncoder.name("load-test");
                    auctionEncoder.description("load test auction");
                    retryOffer(aeronCluster, sendBuffer,
                        MessageHeaderEncoder.ENCODED_LENGTH + auctionEncoder.encodedLength(), running);

                    final long createDeadline = System.currentTimeMillis() + 5_000;
                    while (client.auctionCreationResult == null && System.currentTimeMillis() < createDeadline)
                    {
                        aeronCluster.pollEgress();
                        Thread.yield();
                    }
                    if (client.auctionCreationResult == null)
                    {
                        throw new RuntimeException("No response to CreateAuctionCommand within 5s");
                    }
                    if (client.auctionCreationResult != AddAuctionResult.SUCCESS)
                    {
                        throw new RuntimeException("Auction creation failed: " + client.auctionCreationResult);
                    }
                    LOGGER.info("Setup complete: participants 1+2 and auction 1 created.");
                }
                else
                {
                    if (client.foundAuctionStatus == AuctionStatus.CLOSED)
                    {
                        throw new RuntimeException(
                            "Auction 1 is CLOSED — cluster state must be reset for a fresh auction");
                    }
                    auctionStartTime = client.foundAuctionStartTime;
                    LOGGER.info("Auction 1 found with status={} currentPrice={} — skipping setup.",
                        client.foundAuctionStatus, client.foundAuctionCurrentPrice);
                }

                while (System.currentTimeMillis() < auctionStartTime + 100 && running.get())
                {
                    aeronCluster.pollEgress();
                    Thread.yield();
                }
                LOGGER.info("Auction 1 is now open for bidding.");
            }

            final AddAuctionBidCommandEncoder bidEncoder = new AddAuctionBidCommandEncoder();
            final long nanosPerMsg = publishRate > 0 ? TimeUnit.SECONDS.toNanos(1) / publishRate : Long.MAX_VALUE;
            // Bids must beat the auction's current price, and the auction outlives this client: a
            // restarted client that began again at price 1 would have every bid rejected. Resume
            // from the price the cluster reported (0 for an auction this client just created).
            long seqNum = client.foundAuctionCurrentPrice;
            long sent = 0;
            long nextSendNs = System.nanoTime();
            long nextReportMs = System.currentTimeMillis() + 5_000;

            while (running.get())
            {
                aeronCluster.pollEgress();

                if (publishRate > 0 && client.consecutiveBidFailures >= MAX_CONSECUTIVE_BID_FAILURES)
                {
                    throw new RuntimeException(
                        MAX_CONSECUTIVE_BID_FAILURES + " consecutive bid failures — auction not live");
                }

                final long nowMs = System.currentTimeMillis();
                if (nowMs >= nextReportMs)
                {
                    LOGGER.info("stats: sent={} received={}", sent, client.received.get());
                    nextReportMs = nowMs + 5_000;
                }

                if (publishRate > 0 && System.nanoTime() >= nextSendNs)
                {
                    bidEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder);
                    bidEncoder.auctionId(1L);
                    bidEncoder.addedByParticipantId(2L);
                    bidEncoder.price(seqNum + 1);
                    bidEncoder.correlationId(String.valueOf(seqNum));

                    final int msgLength = MessageHeaderEncoder.ENCODED_LENGTH + bidEncoder.encodedLength();
                    retryOffer(aeronCluster, sendBuffer, msgLength, running);

                    seqNum++;
                    sent++;
                    nextSendNs += nanosPerMsg;
                }
                else if (publishRate == 0)
                {
                    Thread.yield();
                }
            }

            LOGGER.info("Shutdown. sent={} received={}", sent, client.received.get());
        }
        catch (final Exception ex)
        {
            LOGGER.error("ClusterLoadClient failed — exiting for pod restart", ex);
            System.exit(1);
        }
    }

    // The harness starts this client alongside the cluster, which needs a few
    // seconds to elect a leader. A connect attempt times out after the cluster
    // message timeout (5 s); exiting then would leave the retry to kubelet's
    // restart back-off (10 s, then 20 s). Retrying here connects as soon as a
    // leader answers. Any other failure still exits, so it still shows as a
    // container restart.
    private static AeronCluster connectWithRetry(
        final ClusterLoadClient client,
        final String podIp,
        final String ingressEndpoints,
        final String aeronDir,
        final AtomicBoolean running) throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + CONNECT_DEADLINE_MS;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                // A fresh context each time: a failed connect closes the one it was given.
                return AeronCluster.connect(
                    new AeronCluster.Context()
                        .egressListener(client)
                        .egressChannel("aeron:udp?endpoint=" + podIp + ":0")
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ingressEndpoints)
                        .aeronDirectoryName(aeronDir));
            }
            catch (final TimeoutException ex)
            {
                if (!running.get() || System.currentTimeMillis() >= deadline)
                {
                    throw ex;
                }
                LOGGER.info("Cluster not answering yet (attempt {}), retrying: {}", attempt, ex.getMessage());
                Thread.sleep(CONNECT_RETRY_PAUSE_MS);
            }
        }
    }

    private static void retryOffer(
        final AeronCluster cluster,
        final UnsafeBuffer buffer,
        final int length,
        final AtomicBoolean running)
    {
        long result;
        do
        {
            result = cluster.offer(buffer, 0, length);
            if (result < 0)
            {
                cluster.pollEgress();
            }
        }
        while (result < 0 && running.get());
    }

    private static String getenv(final String key, final String defaultValue)
    {
        final String val = System.getenv(key);
        return (val == null || val.isEmpty()) ? defaultValue : val;
    }
}
