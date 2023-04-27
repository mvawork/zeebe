/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.transport.stream.impl;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.transport.stream.api.NoSuchStreamException;
import io.camunda.zeebe.transport.stream.api.RemoteStream.ErrorHandler;
import io.camunda.zeebe.transport.stream.api.RemoteStreamMetrics;
import io.camunda.zeebe.transport.stream.impl.ImmutableStreamRegistry.StreamId;
import io.camunda.zeebe.transport.stream.impl.messages.PushStreamRequest;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A naive implementation to push jobs out, which performs no retries of any kind, but reports
 * errors on failure.
 *
 * @param <P> the payload type to be pushed out
 */
final class RemoteStreamPusher<P extends BufferWriter> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteStreamPusher.class);
  private final RemoteStreamMetrics metrics;
  private final Transport transport;
  private final Executor executor;

  RemoteStreamPusher(
      final Transport transport, final Executor executor, final RemoteStreamMetrics metrics) {
    this.metrics = metrics;
    this.transport = Objects.requireNonNull(transport, "must provide a network transport");
    this.executor = Objects.requireNonNull(executor, "must provide an asynchronous executor");
  }

  /**
   * Pushes the given payload asynchronously to the next stream ID provided by the supplier, until
   * the supplier returns nothing more.
   *
   * <p>The error handler and the stream ID supplier are both guaranteed to be called within the
   * executor context of this streamer.
   *
   * @param payload the payload to push
   * @param errorHandler the error handler to delegate to on unrecoverable errors
   * @param streamIdSupplier a stream ID supplier; should return null to stop retrying
   */
  public void pushAsync(
      final P payload,
      final ErrorHandler<P> errorHandler,
      final Supplier<StreamId> streamIdSupplier) {
    Objects.requireNonNull(payload, "must specify a payload");
    Objects.requireNonNull(errorHandler, "must specify a error handler");
    Objects.requireNonNull(streamIdSupplier, "must specify a stream ID supplier");

    final var handler = new RetryingErrorHandler(errorHandler, streamIdSupplier);
    executor.execute(
        () -> {
          final var streamId = streamIdSupplier.get();
          if (streamId == null) {
            errorHandler.handleError(new NoSuchStreamException(), payload);
            return;
          }

          push(payload, handler, streamId);
        });
  }

  private void push(final P payload, final ErrorHandler<P> errorHandler, final StreamId streamId) {
    final var request = new PushStreamRequest().streamId(streamId.streamId()).payload(payload);
    try {
      transport
          .send(request, streamId.receiver())
          .whenCompleteAsync((ok, error) -> onPush(payload, errorHandler, error), executor);
      LOG.trace("Pushed {} to stream {}", payload, streamId);
    } catch (final Exception e) {
      errorHandler.handleError(e, payload);
    }
  }

  private void onPush(final P payload, final ErrorHandler<P> errorHandler, final Throwable error) {
    if (error != null) {
      errorHandler.handleError(error, payload);
    } else {
      metrics.pushSucceeded();
    }
  }

  /**
   * A small abstraction over the network transport. This allows for better testability, and also
   * removes the need for this class to know how communication occurs (e.g. which topic the message
   * is sent over)
   */
  interface Transport {

    /**
     * Sends the given request out to the given receiver. May throw errors, e.g. serialization
     * errors.
     *
     * @param request the request to send
     * @param receiver the expected target
     * @return a future which is completed when the request has been acknowledged by the receiver,
     *     or an error occurred
     * @throws Exception if an error occurs before the request is sent out, i.e. serialization error
     */
    CompletableFuture<Void> send(final PushStreamRequest request, final MemberId receiver)
        throws Exception;
  }

  private final class RetryingErrorHandler implements ErrorHandler<P> {
    private final ErrorHandler<P> delegate;
    private final Supplier<StreamId> targetSupplier;

    private RetryingErrorHandler(
        final ErrorHandler<P> delegate, final Supplier<StreamId> targetSupplier) {
      this.delegate = delegate;
      this.targetSupplier = targetSupplier;
    }

    @Override
    public void handleError(final Throwable error, final P data) {
      LOG.debug("Failed to push {}", data, error);

      final var nextStreamId = targetSupplier.get();
      if (nextStreamId == null) {
        metrics.pushFailed();
        LOG.debug("Exhausted all streams to push {}", data, error);
        delegate.handleError(error, data);
      } else {
        push(data, this, nextStreamId);
      }
    }
  }
}
