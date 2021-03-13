/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.s3.internal;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.utils.Logger;

/**
 * Publisher of the response data from crt. Tracks outstanding demand and delivers the data to the subscriber
 */
@SdkInternalApi
public final class S3CrtDataPublisher implements SdkPublisher<ByteBuffer> {
    private static final Logger log = Logger.loggerFor(S3CrtDataPublisher.class);
    private static final Object COMPLETE = new Object();
    /**
     * Flag to indicate we are currently delivering events to the subscriber.
     */
    private final AtomicBoolean isDelivering = new AtomicBoolean(false);
    private final Queue<Object> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicLong outstandingDemand = new AtomicLong(0);
    private final AtomicReference<Subscriber<? super ByteBuffer>> subscriberRef = new AtomicReference<>(null);

    private volatile boolean isDone;

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        if (subscriberRef.compareAndSet(null, subscriber)) {
            subscriber.onSubscribe(new DataSubscription());
        }  else {
            log.error(() -> "DataPublisher can only be subscribed to once.");
            throw new IllegalStateException("DataPublisher may only be subscribed to once");
        }
    }

    public void notifyStreamingFinished() {
        buffer.add(COMPLETE);
        flushBuffer();
    }

    public void deliverData(ByteBuffer byteBuffer) {
        buffer.add(byteBuffer);
        flushBuffer();
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }
        // if it's already draining, no op
        if (subscriberRef.get() != null && isDelivering.compareAndSet(false, true)) {
            log.debug(() -> "Publishing data, buffer size: " + buffer.size() + " demand " + outstandingDemand.get());

            while (!buffer.isEmpty() && outstandingDemand.get() > 0) {
                log.debug(() -> "buffer is not empty & there's request demand. " + "Buffer size: " + buffer.size() + " "
                               + "Demand: " + outstandingDemand.get());

                if (COMPLETE.equals(buffer.peek())) {
                    buffer.poll();
                    isDone = true;
                    subscriberRef.get().onComplete();
                } else {
                    ByteBuffer byteBuffer = (ByteBuffer) buffer.poll();
                    outstandingDemand.decrementAndGet();
                    subscriberRef.get().onNext(byteBuffer);
                }
            }
            isDelivering.compareAndSet(true, false);

        } else {
            log.debug(() -> "Already draining, no op");
        }
    }

    private final class DataSubscription implements Subscription {

        @Override
        public void request(long n) {
            if (isDone) {
                return;
            }

            if (n <= 0) {
                subscriberRef.get().onError(new IllegalArgumentException("Request is for <= 0 elements: " + n));
                return;
            }

            addDemand(n);
            log.debug(() -> "Received demand: " + n + ". Total demands: " + outstandingDemand.get());
            flushBuffer();
        }

        @Override
        public void cancel() {
            log.info(() -> "cancelled");
            isDone = true;
            buffer.clear();
            subscriberRef.set(null);

        }

        private void addDemand(long n) {

            outstandingDemand.getAndUpdate(initialDemand -> {
                if (Long.MAX_VALUE - initialDemand < n) {
                    return Long.MAX_VALUE;
                } else {
                    return initialDemand + n;
                }
            });
        }
    }
}

