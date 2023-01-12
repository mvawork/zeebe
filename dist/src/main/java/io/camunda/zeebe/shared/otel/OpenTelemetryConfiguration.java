/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.shared.otel;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public final class OpenTelemetryConfiguration {

  @Bean
  public OpenTelemetrySdk openTelemetrySdk() {
    return AutoConfiguredOpenTelemetrySdk.builder()
        .setResultAsGlobal(true)
        .registerShutdownHook(true)
        .build()
        .getOpenTelemetrySdk();
  }
}
