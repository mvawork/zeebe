/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.JournalException.SegmentSizeTooSmall;
import io.camunda.zeebe.journal.JournalRecord;
import io.camunda.zeebe.util.buffer.BufferWriter;

final class SegmentedJournalWriter {
  private final SegmentsManager segments;
  private final SegmentsFlusher flusher;
  private final JournalMetrics journalMetrics;

  private Segment currentSegment;
  private SegmentWriter currentWriter;

  SegmentedJournalWriter(
      final SegmentsManager segments,
      final SegmentsFlusher flusher,
      final JournalMetrics journalMetrics) {
    this.segments = segments;
    this.flusher = flusher;
    this.journalMetrics = journalMetrics;

    currentSegment = segments.getLastSegment();
    currentWriter = currentSegment.writer();
  }

  long getLastIndex() {
    return currentWriter.getLastIndex();
  }

  long getNextIndex() {
    return currentWriter.getNextIndex();
  }

  JournalRecord append(final long asqn, final BufferWriter recordDataWriter) {
    final var appendResult = currentWriter.append(asqn, recordDataWriter);
    if (appendResult.isRight()) {
      return appendResult.get();
    }

    if (currentSegment.index() == currentWriter.getNextIndex()) {
      throw new SegmentSizeTooSmall("Failed appending, segment size is too small");
    }

    journalMetrics.observeSegmentCreation(this::createNewSegment);
    final var appendResultOnNewSegment = currentWriter.append(asqn, recordDataWriter);
    if (appendResultOnNewSegment.isLeft()) {
      throw appendResultOnNewSegment.getLeft();
    }
    return appendResultOnNewSegment.get();
  }

  void append(final JournalRecord record) {
    final var appendResult = currentWriter.append(record);
    if (appendResult.isRight()) {
      return;
    }

    if (currentSegment.index() == currentWriter.getNextIndex()) {
      throw new SegmentSizeTooSmall("Failed appending, segment size is too small");
    }

    journalMetrics.observeSegmentCreation(this::createNewSegment);
    final var resultInNewSegment = currentWriter.append(record);
    if (resultInNewSegment.isLeft()) {
      throw resultInNewSegment.getLeft();
    }
  }

  void reset(final long index) {
    flusher.setLastFlushedIndex(index - 1);
    currentSegment = segments.resetSegments(index);
    currentWriter = currentSegment.writer();
  }

  void deleteAfter(final long index) {
    // Delete all segments with first indexes greater than the given index.
    while (index < currentSegment.index() && currentSegment != segments.getFirstSegment()) {
      segments.removeSegment(currentSegment);
      currentSegment = segments.getLastSegment();
      currentWriter = currentSegment.writer();
    }

    // Truncate down to the current index, such that the last index is `index`, and the next index
    // `index + 1`
    flusher.setLastFlushedIndex(index);
    // Reset last entry position in descriptor to 0, to ensure that after a restart it is not using
    // the old truncated entry.
    currentSegment.resetLastEntryInDescriptor();
    currentWriter.truncate(index);
  }

  void flush() {
    // even if the next flush index has not been written, this will always flush at least the last
    // segment if only to cover cases such as truncating the log, where the next flush index may not
    // have been written yet but we still want to flush that segment after modifying it
    flusher.flush(segments.getTailSegments(flusher.nextFlushIndex()));
  }

  private void createNewSegment() {
    currentSegment.updateDescriptor();
    currentSegment = segments.getNextSegment();
    currentWriter = currentSegment.writer();
  }
}
