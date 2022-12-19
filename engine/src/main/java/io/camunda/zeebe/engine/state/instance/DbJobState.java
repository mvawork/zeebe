/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.instance;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbCompositeKey;
import io.camunda.zeebe.db.impl.DbForeignKey;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DbNil;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.engine.Loggers;
import io.camunda.zeebe.engine.metrics.JobMetrics;
import io.camunda.zeebe.engine.state.immutable.JobState;
import io.camunda.zeebe.engine.state.mutable.MutableJobState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import io.camunda.zeebe.stream.api.ExternalJobActivator;
import io.camunda.zeebe.stream.api.ExternalJobActivator.Handler;
import io.camunda.zeebe.util.EnsureUtil;
import io.camunda.zeebe.util.buffer.BufferUtil;
import io.prometheus.client.Histogram;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

/**
 * TODO: current downside with the approach of activating from the state is that the record stored
 * is not the record written. I think this is minimal as activation is a best of effort, but it
 * reduces observability for sure.
 *
 * <p>Also, how are we going to calculate/estimate activated jobs without any records for them? We
 * may need to bring back the job ACTIVATED record for observability purposes.
 */
public final class DbJobState implements JobState, MutableJobState {
  private static final Histogram EXTERNAL_ACTIVATOR_LATENCY =
      Histogram.build()
          .namespace("zeebe_job_stream")
          .name("external_activate_latency")
          .help("Time to externally activate a job")
          .register();

  private static final Logger LOG = Loggers.PROCESS_PROCESSOR_LOGGER;

  // key => job record value
  // we need two separate wrapper to not interfere with get and put
  // see https://github.com/zeebe-io/zeebe/issues/1914
  private final JobRecordValue jobRecordToRead = new JobRecordValue();
  private final JobRecordValue jobRecordToWrite = new JobRecordValue();

  private final DbLong jobKey;
  private final DbForeignKey<DbLong> fkJob;
  private final ColumnFamily<DbLong, JobRecordValue> jobsColumnFamily;

  // key => job state
  private final JobStateValue jobState = new JobStateValue();
  private final ColumnFamily<DbForeignKey<DbLong>, JobStateValue> statesJobColumnFamily;

  // type => [key]
  private final DbString jobTypeKey;
  private final DbCompositeKey<DbString, DbForeignKey<DbLong>> typeJobKey;
  private final ColumnFamily<DbCompositeKey<DbString, DbForeignKey<DbLong>>, DbNil>
      activatableColumnFamily;

  // timeout => key
  private final DbLong deadlineKey;
  private final DbCompositeKey<DbLong, DbForeignKey<DbLong>> deadlineJobKey;
  private final ColumnFamily<DbCompositeKey<DbLong, DbForeignKey<DbLong>>, DbNil>
      deadlinesColumnFamily;

  private final DbLong backoffKey;
  private final DbCompositeKey<DbLong, DbForeignKey<DbLong>> backoffJobKey;
  private final ColumnFamily<DbCompositeKey<DbLong, DbForeignKey<DbLong>>, DbNil>
      backoffColumnFamily;
  private long nextBackOffDueDate;

  private final JobMetrics metrics;

  private ExternalJobActivator externalJobActivator;

  public DbJobState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb,
      final TransactionContext transactionContext,
      final int partitionId) {
    jobKey = new DbLong();
    fkJob = new DbForeignKey<>(jobKey, ZbColumnFamilies.JOBS);
    jobsColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.JOBS, transactionContext, jobKey, jobRecordToRead);

    statesJobColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.JOB_STATES, transactionContext, fkJob, jobState);

    jobTypeKey = new DbString();
    typeJobKey = new DbCompositeKey<>(jobTypeKey, fkJob);
    activatableColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.JOB_ACTIVATABLE, transactionContext, typeJobKey, DbNil.INSTANCE);

    deadlineKey = new DbLong();
    deadlineJobKey = new DbCompositeKey<>(deadlineKey, fkJob);
    deadlinesColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.JOB_DEADLINES, transactionContext, deadlineJobKey, DbNil.INSTANCE);

    backoffKey = new DbLong();
    backoffJobKey = new DbCompositeKey<>(backoffKey, fkJob);
    backoffColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.JOB_BACKOFF, transactionContext, backoffJobKey, DbNil.INSTANCE);

    metrics = new JobMetrics(partitionId);
  }

  @Override
  public void create(final long key, final JobRecord record) {
    final DirectBuffer type = record.getTypeBuffer();
    createJob(key, record, type);
  }

  @Override
  public void activate(final long key, final JobRecord record) {
    final DirectBuffer type = record.getTypeBuffer();
    final long deadline = record.getDeadline();

    validateParameters(type);
    EnsureUtil.ensureGreaterThan("deadline", deadline, 0);

    updateJobRecord(key, record);

    updateJobState(State.ACTIVATED);

    makeJobNotActivatable(type);

    addJobDeadline(deadline);
  }

  @Override
  public void recurAfterBackoff(final long key, final JobRecord record) {
    getExternalActivator(record)
        .ifPresentOrElse(
            handler -> activateJobExternally(key, record, handler),
            () -> updateJob(key, record, State.ACTIVATABLE));

    jobKey.wrapLong(key);
    backoffKey.wrapLong(record.getRecurringTime());
    backoffColumnFamily.deleteExisting(backoffJobKey);
  }

  @Override
  public void timeout(final long key, final JobRecord record) {
    final DirectBuffer type = record.getTypeBuffer();
    final long deadline = record.getDeadline();

    validateParameters(type);
    EnsureUtil.ensureGreaterThan("deadline", deadline, 0);

    getExternalActivator(record)
        .ifPresentOrElse(
            handler -> activateJobExternally(key, record, handler),
            () -> updateJob(key, record, State.ACTIVATABLE));
    removeJobDeadline(deadline);
  }

  @Override
  public void complete(final long key, final JobRecord record) {
    delete(key, record);
  }

  @Override
  public void cancel(final long key, final JobRecord record) {
    delete(key, record);
  }

  @Override
  public void disable(final long key, final JobRecord record) {
    updateJob(key, record, State.FAILED);
    makeJobNotActivatable(record.getTypeBuffer());
  }

  @Override
  public void throwError(final long key, final JobRecord updatedValue) {
    updateJob(key, updatedValue, State.ERROR_THROWN);
    makeJobNotActivatable(updatedValue.getTypeBuffer());
  }

  @Override
  public void delete(final long key, final JobRecord record) {
    final DirectBuffer type = record.getTypeBuffer();
    final long deadline = record.getDeadline();

    jobKey.wrapLong(key);
    jobsColumnFamily.deleteExisting(jobKey);

    statesJobColumnFamily.deleteExisting(fkJob);

    makeJobNotActivatable(type);

    removeJobDeadline(deadline);
  }

  @Override
  public void fail(final long key, final JobRecord updatedValue) {
    if (updatedValue.getRetries() > 0) {
      if (updatedValue.getRetryBackoff() > 0) {
        jobKey.wrapLong(key);
        backoffKey.wrapLong(updatedValue.getRecurringTime());
        backoffColumnFamily.insert(backoffJobKey, DbNil.INSTANCE);
        updateJob(key, updatedValue, State.FAILED);
      } else {
        getExternalActivator(updatedValue)
            .ifPresentOrElse(
                handler -> activateJobExternally(key, updatedValue, handler),
                () -> updateJob(key, updatedValue, State.ACTIVATABLE));
      }
    } else {
      updateJob(key, updatedValue, State.FAILED);
      makeJobNotActivatable(updatedValue.getTypeBuffer());
    }
  }

  @Override
  public void resolve(final long key, final JobRecord updatedValue) {
    getExternalActivator(updatedValue)
        .ifPresentOrElse(
            handler -> activateJobExternally(key, updatedValue, handler),
            () -> updateJob(key, updatedValue, State.ACTIVATABLE));
  }

  @Override
  public JobRecord updateJobRetries(final long jobKey, final int retries) {
    final JobRecord job = getJob(jobKey);
    if (job != null) {
      job.setRetries(retries);
      updateJobRecord(jobKey, job);
    }
    return job;
  }

  private void addJobDeadline(final long deadline) {
    deadlineKey.wrapLong(deadline);
    deadlinesColumnFamily.insert(deadlineJobKey, DbNil.INSTANCE);
  }

  private void createJob(final long key, final JobRecord record, final DirectBuffer type) {
    getExternalActivator(record)
        .ifPresentOrElse(
            handler -> {
              final var deadline = ActorClock.currentTimeMillis() + 600_000;
              record.setWorker("push").setDeadline(deadline);
              createJobRecord(key, record);

              initializeJobState(State.ACTIVATED);
              addJobDeadline(deadline);

              try (final var timer = EXTERNAL_ACTIVATOR_LATENCY.startTimer()) {
                handler.handle(key, record);
              }
            },
            () -> {
              createJobRecord(key, record);
              initializeJobState();
              makeJobActivatable(type, key);
            });
  }

  private void activateJobExternally(
      final long key, final JobRecord record, final Handler handler) {
    record.setWorker("push").setDeadline(ActorClock.currentTimeMillis() + 60_000);
    activate(key, record);

    try (final var timer = EXTERNAL_ACTIVATOR_LATENCY.startTimer()) {
      handler.handle(key, record);
    }
  }

  private Optional<Handler> getExternalActivator(final JobRecord record) {
    if (externalJobActivator == null) {
      return Optional.empty();
    }

    // TODO: required for tests, clone the buffer; may not be required for production use cases
    final var type = BufferUtil.cloneBuffer(record.getTypeBuffer());
    return externalJobActivator.activateJob(type);
  }

  private void updateJob(final long key, final JobRecord updatedValue, final State newState) {
    final DirectBuffer type = updatedValue.getTypeBuffer();
    final long deadline = updatedValue.getDeadline();

    validateParameters(type);

    updateJobRecord(key, updatedValue);

    updateJobState(newState);

    if (newState == State.ACTIVATABLE) {
      makeJobActivatable(type, key);
    }

    if (deadline > 0) {
      removeJobDeadline(deadline);
    }
  }

  private void validateParameters(final DirectBuffer type) {
    EnsureUtil.ensureNotNullOrEmpty("type", type);
  }

  @Override
  public void forEachTimedOutEntry(
      final long upperBound, final BiFunction<Long, JobRecord, Boolean> callback) {
    deadlinesColumnFamily.whileTrue(
        (key, value) -> {
          final long deadline = key.first().getValue();
          final boolean isDue = deadline < upperBound;
          if (isDue) {
            final long jobKey1 = key.second().inner().getValue();
            return visitJob(
                jobKey1, callback::apply, () -> deadlinesColumnFamily.deleteExisting(key));
          }
          return false;
        });
  }

  @Override
  public boolean exists(final long jobKey) {
    this.jobKey.wrapLong(jobKey);
    return jobsColumnFamily.exists(this.jobKey);
  }

  @Override
  public State getState(final long key) {
    jobKey.wrapLong(key);

    final JobStateValue storedState = statesJobColumnFamily.get(fkJob);

    if (storedState == null) {
      return State.NOT_FOUND;
    }

    return storedState.getState();
  }

  @Override
  public boolean isInState(final long key, final State state) {
    return getState(key) == state;
  }

  @Override
  public void forEachActivatableJobs(
      final DirectBuffer type, final BiFunction<Long, JobRecord, Boolean> callback) {
    jobTypeKey.wrapBuffer(type);

    activatableColumnFamily.whileEqualPrefix(
        jobTypeKey,
        ((compositeKey, zbNil) -> {
          final long jobKey = compositeKey.second().inner().getValue();
          // TODO #6521 reconsider race condition and whether or not the cleanup task is needed
          return visitJob(jobKey, callback::apply, () -> {});
        }));
  }

  @Override
  public JobRecord getJob(final long key) {
    jobKey.wrapLong(key);
    final JobRecordValue jobState = jobsColumnFamily.get(jobKey);
    return jobState == null ? null : jobState.getRecord();
  }

  @Override
  public void setExternalJobActivator(final ExternalJobActivator externalJobActivator) {
    this.externalJobActivator = externalJobActivator;
  }

  @Override
  public long findBackedOffJobs(final long timestamp, final BiPredicate<Long, JobRecord> callback) {
    nextBackOffDueDate = -1L;
    backoffColumnFamily.whileTrue(
        (key, value) -> {
          final long deadline = key.first().getValue();
          boolean consumed = false;
          if (deadline <= timestamp) {
            final long jobKey = key.second().inner().getValue();
            consumed = visitJob(jobKey, callback, () -> backoffColumnFamily.deleteExisting(key));
          }
          if (!consumed) {
            nextBackOffDueDate = deadline;
          }
          return consumed;
        });
    return nextBackOffDueDate;
  }

  boolean visitJob(
      final long jobKey,
      final BiPredicate<Long, JobRecord> callback,
      final Runnable cleanupRunnable) {
    final JobRecord job = getJob(jobKey);
    if (job == null) {
      LOG.error("Expected to find job with key {}, but no job found", jobKey);
      cleanupRunnable.run();
      return true; // we want to continue with the iteration
    }
    return callback.test(jobKey, job);
  }

  private void notifyJobAvailable(final DirectBuffer jobType) {
    if (externalJobActivator != null) {
      // TODO: required now for tests - unclear if this is a real requirement, probably not
      externalJobActivator.activateJob(BufferUtil.cloneBuffer(jobType));
    }
  }

  private void createJobRecord(final long key, final JobRecord record) {
    jobKey.wrapLong(key);
    // do not persist variables in job state
    jobRecordToWrite.setRecordWithoutVariables(record);
    jobsColumnFamily.insert(jobKey, jobRecordToWrite);
  }

  /** Updates the job record without updating variables */
  private void updateJobRecord(final long key, final JobRecord updatedValue) {
    jobKey.wrapLong(key);
    // do not persist variables in job state
    jobRecordToWrite.setRecordWithoutVariables(updatedValue);
    jobsColumnFamily.update(jobKey, jobRecordToWrite);
  }

  private void initializeJobState() {
    initializeJobState(State.ACTIVATABLE);
  }

  private void initializeJobState(final State state) {
    jobState.setState(state);
    statesJobColumnFamily.insert(fkJob, jobState);
  }

  private void updateJobState(final State newState) {
    jobState.setState(newState);
    statesJobColumnFamily.update(fkJob, jobState);
  }

  private void makeJobActivatable(final DirectBuffer type, final long key) {
    EnsureUtil.ensureNotNullOrEmpty("type", type);

    jobTypeKey.wrapBuffer(type);

    jobKey.wrapLong(key);
    // Need to upsert here because jobs can be marked as failed (and thus made activatable)
    // without activating them first
    activatableColumnFamily.upsert(typeJobKey, DbNil.INSTANCE);

    notifyJobAvailable(type);
  }

  private void makeJobNotActivatable(final DirectBuffer type) {
    EnsureUtil.ensureNotNullOrEmpty("type", type);

    jobTypeKey.wrapBuffer(type);
    activatableColumnFamily.deleteIfExists(typeJobKey);
  }

  private void removeJobDeadline(final long deadline) {
    deadlineKey.wrapLong(deadline);
    deadlinesColumnFamily.deleteIfExists(deadlineJobKey);
  }
}
