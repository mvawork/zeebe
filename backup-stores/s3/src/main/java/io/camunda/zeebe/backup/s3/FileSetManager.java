/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.s3;

import io.camunda.zeebe.backup.api.NamedFileSet;
import io.camunda.zeebe.backup.common.NamedFileSetImpl;
import io.camunda.zeebe.backup.s3.util.CompletableFutureUtils;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** Can save and restore {@link NamedFileSet NamedFileSets}. */
final class FileSetManager {

  private static final Logger LOG = LoggerFactory.getLogger(FileSetManager.class);
  private final S3AsyncClient client;
  private final S3BackupConfig config;

  public FileSetManager(final S3AsyncClient client, final S3BackupConfig config) {
    this.client = client;
    this.config = config;
  }

  CompletableFuture<Void> save(final String prefix, final NamedFileSet files) {
    LOG.debug("Saving {} files to prefix {}", files.files().size(), prefix);
    final var futures =
        files.namedFiles().entrySet().stream()
            .map(segmentFile -> saveFile(prefix, segmentFile.getKey(), segmentFile.getValue()))
            .toList();

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
  }

  private CompletableFuture<PutObjectResponse> saveFile(
      final String prefix, final String fileName, final Path filePath) {
    LOG.trace("Saving file {}({}) in prefix {}", fileName, filePath, prefix);
    return client.putObject(
        put -> put.bucket(config.bucketName()).key(prefix + fileName),
        AsyncRequestBody.fromFile(filePath));
  }

  CompletableFuture<NamedFileSet> restore(
      final String sourcePrefix, final Set<String> fileNames, final Path targetFolder) {
    LOG.debug(
        "Restoring {} files from prefix {} to {}", fileNames.size(), sourcePrefix, targetFolder);
    return CompletableFutureUtils.mapAsync(
            fileNames, fileName -> restoreFile(sourcePrefix, targetFolder, fileName))
        .thenApply(NamedFileSetImpl::new);
  }

  private CompletableFuture<Path> restoreFile(
      final String sourcePrefix, final Path targetFolder, final String fileName) {
    LOG.trace("Restoring file {} from prefix {} to {}", fileName, sourcePrefix, targetFolder);
    final var path = targetFolder.resolve(fileName);
    return client
        .getObject(req -> req.bucket(config.bucketName()).key(sourcePrefix + fileName), path)
        .thenApply(response -> path);
  }
}