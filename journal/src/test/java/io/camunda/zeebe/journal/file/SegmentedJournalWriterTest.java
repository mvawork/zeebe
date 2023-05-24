/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SegmentedJournalWriterTest {
  private final TestJournalFactory journalFactory = new TestJournalFactory();
  private final SegmentsFlusher flusher = new SegmentsFlusher(journalFactory.metaStore());

  private SegmentsManager segments;
  private SegmentedJournalWriter writer;

  @BeforeEach
  void beforeEach(final @TempDir Path tempDir) {
    segments = journalFactory.segmentsManager(tempDir);
    segments.open();
    writer = new SegmentedJournalWriter(segments, flusher, journalFactory.metrics());
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietClose(segments);
  }

  @Test
  void shouldResetLastFlushedIndexOnDeleteAfter() {
    // given
    writer.append(1, journalFactory.entry());
    writer.append(2, journalFactory.entry());
    writer.append(3, journalFactory.entry());
    writer.append(4, journalFactory.entry());
    writer.flush();

    // when
    writer.deleteAfter(2);

    // then
    assertThat(flusher.nextFlushIndex()).isEqualTo(3L);
    assertThat(journalFactory.metaStore().loadLastFlushedIndex()).isEqualTo(2L);
  }

  @Test
  void shouldResetLastFlushedIndexOnReset() {
    // given
    writer.append(1, journalFactory.entry());
    writer.append(2, journalFactory.entry());
    writer.flush();

    // when
    writer.reset(8);

    // then
    assertThat(flusher.nextFlushIndex()).isEqualTo(8L);
    assertThat(journalFactory.metaStore().hasLastFlushedIndex()).isFalse();
  }

  @Test
  void shouldUpdateDescriptor() {
    // given
    while (segments.getFirstSegment() == segments.getLastSegment()) {
      writer.append(-1, journalFactory.entry());
    }
    final var lastIndexInFirstSegment = segments.getLastSegment().index() - 1;

    // when
    segments.close();
    segments.open();

    // then
    final SegmentDescriptor descriptor = segments.getFirstSegment().descriptor();
    assertThat(descriptor.lastIndex()).isEqualTo(lastIndexInFirstSegment);
    assertThat(descriptor.lastPosition()).isNotZero();
  }

  @Test
  void shouldResetToLastEntryEvenIfLastPositionInDescriptorIsIncorrect() throws IOException {
    // given
    while (segments.getFirstSegment() == segments.getLastSegment()) {
      writer.append(-1, journalFactory.entry());
    }
    final var lastIndexInFirstSegment = segments.getLastSegment().index() - 1;

    final SegmentDescriptor descriptorToCorrupt = segments.getFirstSegment().descriptor();
    final Segment firstSegment = segments.getFirstSegment();
    descriptorToCorrupt.setLastPosition(firstSegment.descriptor().lastPosition() + 1);
    final var segmentFile = firstSegment.file().file().toPath();
    try (final FileChannel channel =
        FileChannel.open(segmentFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final MappedByteBuffer buffer =
          channel.map(MapMode.READ_WRITE, 0, descriptorToCorrupt.length());
      descriptorToCorrupt.updateIfCurrentVersion(buffer);
    }

    // when
    segments.close();
    segments.open();

    // then
    assertThat(segments.getFirstSegment().lastIndex()).isEqualTo(lastIndexInFirstSegment);
  }
}
