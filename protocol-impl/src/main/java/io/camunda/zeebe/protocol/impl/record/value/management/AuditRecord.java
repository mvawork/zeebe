/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.record.value.management;

import io.camunda.zeebe.msgpack.property.BinaryProperty;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.value.management.AuditRecordValue;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;

public final class AuditRecord extends UnifiedRecordValue implements AuditRecordValue {
  private static final String EVENTS_KEY = "events";

  private final BinaryProperty events = new BinaryProperty(EVENTS_KEY);

  public AuditRecord() {
    declareProperty(events);
  }

  @Override
  public ByteBuffer getEvents() {
    final ByteBuffer bb =
        events.getValue().byteBuffer() == null
            ? ByteBuffer.wrap(
                events.getValue().byteArray(),
                events.getValue().wrapAdjustment(),
                events.getValue().capacity())
            : events
                .getValue()
                .byteBuffer()
                .asReadOnlyBuffer()
                .position(events.getValue().wrapAdjustment())
                .limit(events.getValue().wrapAdjustment() + events.getValue().capacity());

    return bb;
  }

  public AuditRecord setEvents(DirectBuffer buffer) {
    events.setValue(buffer);
    return this;
  }
}
