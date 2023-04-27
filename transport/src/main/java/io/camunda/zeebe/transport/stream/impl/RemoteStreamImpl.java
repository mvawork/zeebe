/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.transport.stream.impl;

import io.camunda.zeebe.transport.stream.api.RemoteStream;
import io.camunda.zeebe.transport.stream.impl.ImmutableStreamRegistry.AggregatedRemoteStream;
import io.camunda.zeebe.transport.stream.impl.ImmutableStreamRegistry.StreamConsumer;
import io.camunda.zeebe.transport.stream.impl.ImmutableStreamRegistry.StreamId;
import io.camunda.zeebe.util.buffer.BufferReader;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RemoteStreamImpl<M extends BufferReader, P extends BufferWriter>
    implements RemoteStream<M, P> {
  private final AggregatedRemoteStream<M> stream;
  private final RemoteStreamPusher<P> streamer;

  public RemoteStreamImpl(
      final AggregatedRemoteStream<M> stream, final RemoteStreamPusher<P> streamer) {
    this.stream = stream;
    this.streamer = streamer;
  }

  @Override
  public M metadata() {
    return stream.getLogicalId().metadata();
  }

  @Override
  public void push(final P payload, final ErrorHandler<P> errorHandler) {
    streamer.pushAsync(payload, errorHandler, new RandomStreamIdSupplier<>(stream));
  }

  private static final class RandomStreamIdSupplier<M> implements Supplier<StreamId> {
    private final AggregatedRemoteStream<M> stream;
    private final Set<StreamId> attempted = new HashSet<>();

    private RandomStreamIdSupplier(AggregatedRemoteStream<M> stream) {
      this.stream = stream;
    }

    @Override
    public StreamId get() {
      // TODO: might work better if consumers are a set instead of a list
      final var consumers =
          stream.getStreamConsumers().stream().map(StreamConsumer::id).collect(Collectors.toList());
      consumers.removeAll(attempted);
      Collections.shuffle(consumers);

      if (consumers.isEmpty()) {
        return null;
      }

      final var streamId = consumers.get(0);
      attempted.add(streamId);

      return streamId;
    }
  }
}
