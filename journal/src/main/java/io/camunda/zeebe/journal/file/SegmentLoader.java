/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.CorruptedJournalException;
import io.camunda.zeebe.journal.JournalException;
import io.camunda.zeebe.journal.file.PosixFs.Advice;
import io.camunda.zeebe.util.FileUtil;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create a segment file. Load a segment from the segment file. */
final class SegmentLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentLoader.class);
  private static final ByteOrder ENDIANNESS = ByteOrder.LITTLE_ENDIAN;

  // TODO: make injectable
  private final PosixFs posixFs = new PosixFs();
  private final SegmentAllocator allocator;

  SegmentLoader() {
    this(SegmentAllocator.fill());
  }

  SegmentLoader(final SegmentAllocator allocator) {
    this.allocator = allocator;
  }

  Segment createSegment(
      final Path segmentFile,
      final SegmentDescriptor descriptor,
      final long lastWrittenIndex,
      final long lastWrittenAsqn,
      final JournalIndex journalIndex) {
    final MappedByteBuffer mappedSegment;

    try {
      mappedSegment = mapNewSegment(segmentFile, descriptor, lastWrittenIndex);
    } catch (final IOException e) {
      throw new JournalException(
          String.format("Failed to create new segment file %s", segmentFile), e);
    }

    try {
      descriptor.copyTo(mappedSegment);
      mappedSegment.force();
    } catch (final InternalError e) {
      throw new JournalException(
          String.format(
              "Failed to ensure durability of segment %s with descriptor %s, rolling back",
              segmentFile, descriptor),
          e);
    }

    // while flushing the file's contents ensures its data is present on disk on recovery, it's also
    // necessary to flush the directory to ensure that the file itself is visible as an entry of
    // that directory after recovery
    try {
      FileUtil.flushDirectory(segmentFile.getParent());
    } catch (final IOException e) {
      throw new JournalException(
          String.format("Failed to flush journal directory after creating segment %s", segmentFile),
          e);
    }

    return loadSegment(
        segmentFile, mappedSegment, descriptor, lastWrittenIndex, lastWrittenAsqn, journalIndex);
  }

  Segment loadExistingSegment(
      final Path segmentFile,
      final long lastWrittenIndex,
      final long lastWrittenAsqn,
      final JournalIndex journalIndex) {
    final var descriptor = readDescriptor(segmentFile);
    final MappedByteBuffer mappedSegment;

    try (final var channel =
        FileChannel.open(segmentFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      mappedSegment = mapSegment(channel, descriptor.maxSegmentSize());
    } catch (final IOException e) {
      throw new JournalException(
          String.format("Failed to load existing segment %s", segmentFile), e);
    }

    return loadSegment(
        segmentFile, mappedSegment, descriptor, lastWrittenIndex, lastWrittenAsqn, journalIndex);
  }

  /* ---- Internal methods ------ */
  private Segment loadSegment(
      final Path file,
      final MappedByteBuffer buffer,
      final SegmentDescriptor descriptor,
      final long lastWrittenIndex,
      final long lastWrittenAsqn,
      final JournalIndex journalIndex) {
    final SegmentFile segmentFile = new SegmentFile(file.toFile());
    return new Segment(
        segmentFile, descriptor, buffer, lastWrittenIndex, lastWrittenAsqn, journalIndex);
  }

  private MappedByteBuffer mapSegment(final FileChannel channel, final long segmentSize)
      throws IOException {
    final var mappedSegment = channel.map(MapMode.READ_WRITE, 0, segmentSize);
    mappedSegment.order(ENDIANNESS);
    madvise(mappedSegment, segmentSize);

    return mappedSegment;
  }

  private SegmentDescriptor readDescriptor(final Path file) {
    final var fileName = file.getFileName().toString();

    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      final var fileSize = Files.size(file);
      final byte version = readVersion(channel, fileName);
      final int length = SegmentDescriptor.getEncodingLengthForVersion(version);
      if (fileSize < length) {
        throw new CorruptedJournalException(
            String.format(
                "Expected segment '%s' with version %d to be at least %d bytes long but it only has %d.",
                fileName, version, length, fileSize));
      }

      final ByteBuffer buffer = ByteBuffer.allocate(length);
      final int readBytes = channel.read(buffer, 0);

      if (readBytes != -1 && readBytes < length) {
        throw new JournalException(
            String.format(
                "Expected to read %d bytes of segment '%s' with %d version but only read %d bytes.",
                length, fileName, version, readBytes));
      }

      buffer.flip();
      return new SegmentDescriptor(buffer);
    } catch (final IndexOutOfBoundsException e) {
      throw new JournalException(
          String.format(
              "Expected to read descriptor of segment '%s', but nothing was read.", fileName),
          e);
    } catch (final UnknownVersionException e) {
      throw new CorruptedJournalException(
          String.format("Couldn't read or recognize version of segment '%s'.", fileName), e);
    } catch (final IOException e) {
      throw new JournalException(e);
    }
  }

  private byte readVersion(final FileChannel channel, final String fileName) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(1);
    final int readBytes = channel.read(buffer);

    if (readBytes == 0) {
      throw new JournalException(
          String.format(
              "Expected to read the version byte from segment '%s' but nothing was read.",
              fileName));
    } else if (readBytes == -1) {
      throw new CorruptedJournalException(
          String.format(
              "Expected to read the version byte from segment '%s' but got EOF instead.",
              fileName));
    }

    return buffer.get(0);
  }

  private MappedByteBuffer mapNewSegment(
      final Path segmentPath, final SegmentDescriptor descriptor, final long lastWrittenIndex)
      throws IOException {
    final var maxSegmentSize = descriptor.maxSegmentSize();

    if (Files.exists(segmentPath)) {
      // do not reuse a segment into which we've already written!
      if (lastWrittenIndex >= descriptor.index()) {
        throw new JournalException(
            String.format(
                "Failed to create journal segment %s, as it already exists, and the last written "
                    + "index %d indicates we've already written to it",
                segmentPath, lastWrittenIndex));
      }

      LOGGER.warn(
          "Failed to create segment {}: an unused file already existed, and will be replaced",
          segmentPath);
      Files.delete(segmentPath);
      return mapNewSegment(segmentPath, descriptor, lastWrittenIndex);
    }

    try (final var file = new RandomAccessFile(segmentPath.toFile(), "rw")) {
      allocator.allocate(file.getFD(), file.getChannel(), maxSegmentSize);
      return mapSegment(file.getChannel(), maxSegmentSize);
    }
  }

  private void madvise(final MappedByteBuffer mappedSegment, final long segmentSize) {
    if (!posixFs.isPosixMadviseEnabled()) {
      return;
    }

    try {
      posixFs.madvise(mappedSegment, segmentSize, Advice.POSIX_MADV_SEQUENTIAL);
    } catch (final UnsupportedOperationException e) {
      LOGGER.warn(
          "Failed to use native system call to advise filesystem, will use fallback from now on",
          e);
      posixFs.disablePosixMadvise();
    }
  }
}
