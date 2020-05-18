/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties;
import feast.core.config.FeastProperties.JobProperties;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.JobMatcher;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Store;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListStoresResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetMeta;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class JobCoordinatorServiceTest {

  @Rule public final ExpectedException exception = ExpectedException.none();
  @Mock JobRepository jobRepository;
  @Mock JobManager jobManager;
  @Mock SpecService specService;
  @Mock FeatureSetRepository featureSetRepository;

  private FeastProperties feastProperties;

  @Before
  public void setUp() {
    initMocks(this);
    feastProperties = new FeastProperties();
    JobProperties jobProperties = new JobProperties();
    jobProperties.setJobUpdateTimeoutSeconds(5);
    feastProperties.setJobs(jobProperties);
  }

  @Test
  public void shouldDoNothingIfNoStoresFound() throws InvalidProtocolBufferException {
    when(specService.listStores(any())).thenReturn(ListStoresResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldDoNothingIfNoMatchingFeatureSetsFound() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("*").setName("*").build())
            .build();
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());
    when(specService.listFeatureSets(
            Filter.newBuilder().setProject("*").setFeatureSetName("*").build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().build());
    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();
    verify(jobRepository, times(0)).saveAndFlush(any());
  }

  @Test
  public void shouldGenerateAndSubmitJobsIfAny() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    Source source =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSetProto1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetProto1);
    FeatureSetProto.FeatureSet featureSetProto2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetProto2);
    String extId = "ext";
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    Job expectedInput =
        Job.builder()
            .id("")
            .extId("")
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source))
            .store(Store.fromProto(store))
            .status(JobStatus.PENDING)
            .build();
    expectedInput.addFeatureSet(featureSet1);
    expectedInput.addFeatureSet(featureSet2);

    Job expected =
        Job.builder()
            .id("some_id")
            .extId(extId)
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source))
            .store(Store.fromProto(store))
            .status(JobStatus.RUNNING)
            .build();
    expected.addFeatureSet(featureSet1);
    expected.addFeatureSet(featureSet2);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));
    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput)))).thenReturn(expected);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();
    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();
    assertThat(actual, equalTo(Collections.singletonList(expected)));
  }

  @Test
  public void shouldGroupJobsBySource() throws InvalidProtocolBufferException {
    StoreProto.Store store =
        StoreProto.Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().build())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("*").build())
            .build();
    Source source1 =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("servers:9092")
                    .build())
            .build();
    Source source2 =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setTopic("topic")
                    .setBootstrapServers("other.servers:9092")
                    .build())
            .build();

    FeatureSetProto.FeatureSet featureSetProto1 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source1)
                    .setProject("project1")
                    .setName("features1"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet1 = FeatureSet.fromProto(featureSetProto1);

    FeatureSetProto.FeatureSet featureSetProto2 =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(
                FeatureSetSpec.newBuilder()
                    .setSource(source2)
                    .setProject("project1")
                    .setName("features2"))
            .setMeta(FeatureSetMeta.newBuilder())
            .build();
    FeatureSet featureSet2 = FeatureSet.fromProto(featureSetProto2);

    Job expectedInput1 =
        Job.builder()
            .id("name1")
            .extId("")
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source1))
            .store(Store.fromProto(store))
            .status(JobStatus.PENDING)
            .build();
    expectedInput1.addFeatureSet(featureSet1);

    Job expected1 =
        Job.builder()
            .id("name1")
            .extId("extId1")
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source1))
            .store(Store.fromProto(store))
            .status(JobStatus.RUNNING)
            .build();
    expected1.addFeatureSet(featureSet1);

    Job expectedInput2 =
        Job.builder()
            .id("name2")
            .extId("")
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source2))
            .store(Store.fromProto(store))
            .status(JobStatus.PENDING)
            .build();
    expectedInput2.addFeatureSet(featureSet2);

    Job expected2 =
        Job.builder()
            .id("name2")
            .extId("extId2")
            .runner(Runner.DATAFLOW)
            .source(feast.core.model.Source.fromProto(source2))
            .store(Store.fromProto(store))
            .status(JobStatus.RUNNING)
            .build();
    expected2.addFeatureSet(featureSet2);
    ArgumentCaptor<List<Job>> jobArgCaptor = ArgumentCaptor.forClass(List.class);

    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "project1"))
        .thenReturn(Lists.newArrayList(featureSet1, featureSet2));

    when(specService.listStores(any()))
        .thenReturn(ListStoresResponse.newBuilder().addStore(store).build());

    when(jobManager.startJob(argThat(new JobMatcher(expectedInput1)))).thenReturn(expected1);
    when(jobManager.startJob(argThat(new JobMatcher(expectedInput2)))).thenReturn(expected2);
    when(jobManager.getRunnerType()).thenReturn(Runner.DATAFLOW);

    JobCoordinatorService jcs =
        new JobCoordinatorService(
            jobRepository, featureSetRepository, specService, jobManager, feastProperties);
    jcs.Poll();

    verify(jobRepository, times(1)).saveAll(jobArgCaptor.capture());
    List<Job> actual = jobArgCaptor.getValue();

    assertThat(actual.get(0), equalTo(expected1));
    assertThat(actual.get(1), equalTo(expected2));
  }
}
