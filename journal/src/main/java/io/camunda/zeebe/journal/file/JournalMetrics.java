/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
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
package io.camunda.zeebe.journal.file;

import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Timer;
import io.prometheus.client.Histogram;

final class JournalMetrics {
  private static final String NAMESPACE = "atomix";
  private static final String PARTITION_LABEL = "partition";
  private static final Histogram NEXT_SEGMENT_CREATION_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("next_segment_creation_time")
          .help(
              "Time spent creating a new segment for the writer, including flush the previous one")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Histogram SEGMENT_CREATION_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_creation_time")
          .help("Time spent to create a new segment")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Histogram SEGMENT_TRUNCATE_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_truncate_time")
          .help("Time spend to truncate a segment")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Histogram SEGMENT_FLUSH_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_flush_time")
          .help("Time spend to flush segment to disk")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Gauge SEGMENT_COUNT =
      Gauge.build()
          .namespace(NAMESPACE)
          .name("segment_count")
          .help("Number of segments")
          .labelNames(PARTITION_LABEL)
          .register();
  private static final Gauge JOURNAL_OPEN_DURATION =
      Gauge.build()
          .namespace(NAMESPACE)
          .name("journal_open_time")
          .help("Time taken to open the journal")
          .labelNames(PARTITION_LABEL)
          .register();
  private static final Histogram SEGMENT_ALLOCATE_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_allocate_time")
          .help("Time spent to allocate segment on disk")
          .labelNames(PARTITION_LABEL)
          .register();

  private final Histogram.Child segmentCreationTime;
  private final Histogram.Child nextSegmentCreationTime;
  private final Histogram.Child segmentFlushTime;
  private final Histogram.Child segmentTruncateTime;
  private final Histogram.Child segmentAllocateTime;
  private final Gauge.Child journalOpenTime;
  private final Gauge.Child segmentCount;

  JournalMetrics(final String logName) {
    nextSegmentCreationTime = NEXT_SEGMENT_CREATION_TIME.labels(logName);
    segmentCreationTime = SEGMENT_CREATION_TIME.labels(logName);
    segmentFlushTime = SEGMENT_FLUSH_TIME.labels(logName);
    segmentTruncateTime = SEGMENT_TRUNCATE_TIME.labels(logName);
    segmentAllocateTime = SEGMENT_ALLOCATE_TIME.labels(logName);
    journalOpenTime = JOURNAL_OPEN_DURATION.labels(logName);
    segmentCount = SEGMENT_COUNT.labels(logName);
  }

  void observeNextSegmentCreation(final Runnable segmentCreation) {
    nextSegmentCreationTime.time(segmentCreation);
  }

  void observeSegmentFlush(final Runnable segmentFlush) {
    segmentFlushTime.time(segmentFlush);
  }

  void observeSegmentTruncation(final Runnable segmentTruncation) {
    segmentTruncateTime.time(segmentTruncation);
  }

  Timer startJournalOpenDurationTimer() {
    return journalOpenTime.startTimer();
  }

  void incSegmentCount() {
    segmentCount.inc();
  }

  void decSegmentCount() {
    segmentCount.dec();
  }

  public Histogram.Timer timeSegmentAllocation() {
    return segmentAllocateTime.startTimer();
  }

  public Histogram.Timer timeSegmentCreation() {
    return segmentCreationTime.startTimer();
  }
}
