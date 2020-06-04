/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.job.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import feast.databricks.types.*;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class DatabricksJobManager implements JobManager {
    private final Runner RUNNER_TYPE = Runner.DATABRICKS;

    private final String databricksHost;
    private final String databricksToken;
    private final Map<String, String> defaultOptions;
    private final MetricsProperties metricsProperties;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final int maxRetries = -1;

    public DatabricksJobManager(
            Map<String, String> runnerConfigOptions,
            MetricsProperties metricsProperties,
            String token,
            HttpClient httpClient) {

        this.databricksHost = runnerConfigOptions.get("databricksHost");
        this.defaultOptions = runnerConfigOptions;
        this.metricsProperties = metricsProperties;
        this.httpClient = httpClient;
        this.databricksToken = token;

    }

    @Override
    public Runner getRunnerType() {
        return RUNNER_TYPE;
    }

    @Override
    public Job startJob(Job job) {
        try {
            List<FeatureSetProto.FeatureSet> featureSetProtos = new ArrayList<>();
            for (FeatureSet featureSet : job.getFeatureSets()) {
                featureSetProtos.add(featureSet.toProto());
            }

            String databricksJobId = createDatabricksJob(job.getId());
            return runDatabricksJob(job.getId(), databricksJobId, featureSetProtos, job.getSource().toProto(), job.getStore().toProto());

        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(
                    String.format(
                            "DatabricksJobManager failed to START job with id '%s' because the job"
                                    + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
                            job.getId(), e.getMessage()));
        }
    }

    /**
     * Update an existing Databricks job.
     *
     * @param job job of target job to change
     * @return Databricks-specific job id
     */
    @Override
    public Job updateJob(Job job) {
        return restartJob(job);
    }

    @Override
    public void abortJob(String jobId) {
    }

    @Override
    public Job restartJob(Job job) {
        abortJob(job.getExtId());
        return startJob(job);
    }

    @Override
    public JobStatus getJobStatus(Job job) {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        String.format(
                                                "%s/api/2.0/jobs/runs/get?run_id=%s", this.databricksHost, job.getExtId())))
                        .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                        .build();
        try {
            HttpResponse<String> response =
                    this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == HttpStatus.SC_OK) {
                JsonNode parent = mapper.readTree(response.body());
                Optional<JsonNode> resultState =
                        Optional.ofNullable(parent.path("state").get("result_state"));
                String lifeCycleState = parent.path("state").get("life_cycle_state").asText().toUpperCase();

                if (resultState.isPresent()) {
                    return DatabricksJobStateMapper.map(
                            String.format("%s_%s", lifeCycleState, resultState.get().asText().toUpperCase()));
                }

                return DatabricksJobStateMapper.map(lifeCycleState);
            } else {
                throw new HttpException(
                        String.format("Databricks returned with unexpected code: %s", response.statusCode()));
            }
        } catch (IOException | InterruptedException | HttpException ex) {
            log.error(
                    "Unable to retrieve status of a dabatricks run with id : {}\ncause: {}",
                    job.getExtId(),
                    ex.getMessage());
        }

        return JobStatus.UNKNOWN;
    }

    private String createDatabricksJob(
            String jobId
    ) {


        CreateRequest createRequest = getJobRequest(jobId);

        try {
            String body = mapper.writeValueAsString(createRequest);

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(String.format("%s/api/2.0/jobs/create", this.databricksHost)))
                            .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();

            HttpResponse<String> response =
                    this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == HttpStatus.SC_OK) {
                CreateResponse createResponse = mapper.readValue(response.body(), CreateResponse.class);

                return String.valueOf(createResponse.getJob_id());
            } else {
                throw new HttpException(
                        String.format("Databricks returned with unexpected code: %s", response.statusCode()));
            }
        } catch (IOException | InterruptedException | HttpException e) {
            log.error("Unable to run databricks job with id : {}\ncause: {}", jobId, e.getMessage());
            throw new JobExecutionException(
                    String.format("Unable to run databricks job with id : %s\ncause: %s", jobId, e), e);
        }
    }

    private Job runDatabricksJob(
            String jobId,
            String databricksJobId,
            List<FeatureSetProto.FeatureSet> featureSetProtos,
            SourceProto.Source source,
            StoreProto.Store sink
    ) {
        RunNowRequest runNowRequest = getRunNowRequest(databricksJobId);

        List<FeatureSet> featureSets = new ArrayList<>();
        for (FeatureSetProto.FeatureSet featureSetProto : featureSetProtos) {
            featureSets.add(FeatureSet.fromProto(featureSetProto));
        }

        try {
            String body = mapper.writeValueAsString(runNowRequest);

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(String.format("%s/api/2.0/jobs/run-now", this.databricksHost)))
                            .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();

            HttpResponse<String> response =
                    this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == HttpStatus.SC_OK) {
                RunNowResponse runNowResponse = mapper.readValue(response.body(), RunNowResponse.class);
                Job job = new Job(
                        jobId,
                        String.valueOf(runNowResponse.getRunId()),
                        getRunnerType(),
                        Source.fromProto(source),
                        Store.fromProto(sink),
                        featureSets,
                        JobStatus.RUNNING);

                waitForJobToRun(job);

                return job;
            } else {
                throw new HttpException(
                        String.format("Databricks returned with unexpected code: %s", response.statusCode()));
            }
        } catch (Exception e) {
            log.error("Unable to run databricks job with id : {}\ncause: {}", databricksJobId, e.getMessage());
            throw new JobExecutionException(
                    String.format("Unable to run databricks job with id : %s\ncause: %s", databricksJobId, e), e);
        }

    }

    private RunNowRequest getRunNowRequest(String databricksJobId) {
        // TODO: investigate whats required for spark submit + jar params. (store config + featureset proto definitions?)

        RunNowRequest runNowRequest = new RunNowRequest();
        SparkSubmitParams sparkSubmitParams = new SparkSubmitParams();
        JarParams jarParams = new JarParams();

        sparkSubmitParams.setMain_class_name(this.defaultOptions.get("sparkMainClassName"));
        jarParams.setKafka_broker(this.defaultOptions.get("kafkaBroker"));
        jarParams.setTopic_name(this.defaultOptions.get("kafkaTopicName"));


        runNowRequest.setJob_id(Integer.parseInt(databricksJobId));
        runNowRequest.setSpark_submit_params(sparkSubmitParams);
        runNowRequest.setJar_params(jarParams);
        return runNowRequest;
    }


    private CreateRequest getJobRequest(String jobId) {
        NewCluster newCluster = new NewCluster();
        newCluster.setNum_workers(this.defaultOptions.get("sparkNumWorkers"));
        newCluster.setSpark_version(this.defaultOptions.get("sparkVersion"));
        newCluster.setNode_type_id(this.defaultOptions.get("sparkNodeTypeId"));

        List<Library> libraries = new ArrayList<>();
        Library library = new Library();
        library.setJar(this.defaultOptions.get("jarLocation"));
        libraries.add(library);

        SparkJarTask sparkJarTask = new SparkJarTask();
        sparkJarTask.setMain_class_name(this.defaultOptions.get("sparkMainClassName"));

        CreateRequest createRequest = new CreateRequest();
        createRequest.setName(jobId);
        createRequest.setNew_cluster(newCluster);
        createRequest.setLibraries(libraries);
        createRequest.setSpark_jar_task(sparkJarTask);
        createRequest.setMax_retries(maxRetries);
        return createRequest;
    }

    private void waitForJobToRun(Job job) throws InterruptedException {
        while (true) {
            JobStatus jobStatus = this.getJobStatus(job);
            if (jobStatus.isTerminal()) {
                throw new RuntimeException();
            } else if (jobStatus.equals(JobStatus.RUNNING)) {
                break;
            }
            Thread.sleep(2000);

        }
    }


}
