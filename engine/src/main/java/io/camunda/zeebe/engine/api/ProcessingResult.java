/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.api;

import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter;
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter;

/**
 * Here the interface is just a suggestion. Can be whatever PDT teams thinks is best to work with
 */
public interface ProcessingResult {

  long writeRecordsToStream(LogStreamRecordWriter logStreamRecordWriter);

  boolean writeResponse(CommandResponseWriter commandResponseWriter);

  void executePostCommitTasks();
}