/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.partitioning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.raft.partition.RaftPartitionGroupConfig;
import io.camunda.zeebe.broker.clustering.ClusterServices;
import io.camunda.zeebe.broker.exporter.repo.ExporterRepository;
import io.camunda.zeebe.broker.jobstream.JobPusher;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.monitoring.BrokerHealthCheckService;
import io.camunda.zeebe.protocol.impl.encoding.BrokerInfo;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.util.Environment;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class PartitionManagerImplTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private Environment environment;

  @Mock private ClusterServices mockClusterServices;
  @Mock private ClusterMembershipService mockMembershipService;
  @Mock private Member mockMember;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private JobPusher mockJobPusher;

  @Before
  public void setUp() {
    environment = new Environment();

    when(mockClusterServices.getMembershipService()).thenReturn(mockMembershipService);
  }

  @Test
  public void shouldDisableExplicitFlush() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getExperimental().setDisableExplicitRaftFlush(true);

    // when
    final var partitionManager =
        new PartitionManagerImpl(
            mock(ActorSchedulingService.class),
            brokerConfig,
            new BrokerInfo(1, "dummy"),
            mockClusterServices,
            mock(BrokerHealthCheckService.class),
            null,
            new ArrayList<>(),
            null,
            mock(ExporterRepository.class),
            null,
            mockJobPusher);

    // then
    final var config = getPartitionGroupConfig(partitionManager);
    assertThat(config.getStorageConfig().shouldFlushExplicitly()).isFalse();
  }

  @Test
  public void shouldEnableExplicitFlush() {
    // given
    final var brokerConfig = newConfig();
    brokerConfig.getExperimental().setDisableExplicitRaftFlush(false);

    // when
    final var partitionManager =
        new PartitionManagerImpl(
            mock(ActorSchedulingService.class),
            brokerConfig,
            new BrokerInfo(1, "dummy"),
            mockClusterServices,
            mock(BrokerHealthCheckService.class),
            null,
            new ArrayList<>(),
            null,
            mock(ExporterRepository.class),
            null,
            mockJobPusher);
    // then
    final var config = getPartitionGroupConfig(partitionManager);
    assertThat(config.getStorageConfig().shouldFlushExplicitly()).isTrue();
  }

  private RaftPartitionGroupConfig getPartitionGroupConfig(
      final PartitionManager partitionManager) {
    return (RaftPartitionGroupConfig) partitionManager.getPartitionGroup().config();
  }

  private BrokerCfg newConfig() {
    final var config = new BrokerCfg();
    config.init(temporaryFolder.getRoot().getAbsolutePath(), environment);

    return config;
  }
}
