import os
import uuid
from typing import cast
from urllib.parse import urlparse

from google.api_core.operation import Operation
from google.cloud import dataproc_v1, storage
from google.cloud.dataproc_v1 import Job as DataprocJob
from google.cloud.dataproc_v1 import JobStatus

from feast.pyspark.abc import (
    IngestionJob,
    IngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJobFailure,
    SparkJobParameters,
    SparkJobStatus,
)


class DataprocJobMixin:
    def __init__(self, operation: Operation):
        """
        :param operation: (google.api.core.operation.Operation): A Future for the spark job result,
                returned by the dataproc client.
        """
        self._operation = operation

    def get_id(self) -> str:
        return self._operation.metadata.job_id

    def get_status(self) -> SparkJobStatus:
        if self._operation.running():
            return SparkJobStatus.IN_PROGRESS

        job = cast(DataprocJob, self._operation.result())
        status = cast(JobStatus, job.status)
        if status.state == JobStatus.State.DONE:
            return SparkJobStatus.COMPLETED

        return SparkJobStatus.FAILED


class DataprocRetrievalJob(DataprocJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a Dataproc cluster
    """

    def __init__(self, operation: Operation, output_file_uri: str):
        """
        This is the returned historical feature retrieval job result for DataprocClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(operation)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None):
        try:
            self._operation.result(timeout_sec)
        except Exception as err:
            raise SparkJobFailure(err)
        return self._output_file_uri


class DataprocIngestionJob(DataprocJobMixin, IngestionJob):
    """
    Ingestion job result for a Dataproc cluster
    """


class DataprocClusterLauncher(JobLauncher):
    """
    Submits jobs to an existing Dataproc cluster. Depends on google-cloud-dataproc and
    google-cloud-storage, which are optional dependencies that the user has to installed in
    addition to the Feast SDK.
    """

    def __init__(
        self, cluster_name: str, staging_location: str, region: str, project_id: str,
    ):
        """
        Initialize a dataproc job controller client, used internally for job submission and result
        retrieval.

        Args:
            cluster_name (str):
                Dataproc cluster name.
            staging_location (str):
                GCS directory for the storage of files generated by the launcher, such as the pyspark scripts.
            region (str):
                Dataproc cluster region.
            project_id (str:
                GCP project id for the dataproc cluster.
        """

        self.cluster_name = cluster_name

        scheme, self.staging_bucket, self.remote_path, _, _, _ = urlparse(
            staging_location
        )
        if scheme != "gs":
            raise ValueError(
                "Only GCS staging location is supported for DataprocLauncher."
            )
        self.project_id = project_id
        self.region = region
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

    def _stage_files(self, pyspark_script: str, job_id: str) -> str:
        client = storage.Client()
        bucket = client.get_bucket(self.staging_bucket)
        blob_path = os.path.join(
            self.remote_path, job_id, os.path.basename(pyspark_script),
        )
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(pyspark_script)

        return f"gs://{self.staging_bucket}/{blob_path}"

    def dataproc_submit(self, job_params: SparkJobParameters) -> Operation:
        local_job_id = str(uuid.uuid4())
        pyspark_gcs = self._stage_files(job_params.get_main_file_path(), local_job_id)
        job_config = {
            "reference": {"job_id": local_job_id},
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": pyspark_gcs,
                "args": job_params.get_arguments(),
            },
        }
        return self.job_client.submit_job_as_operation(
            request={
                "project_id": self.project_id,
                "region": self.region,
                "job": job_config,
            }
        )

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        return DataprocRetrievalJob(
            self.dataproc_submit(job_params), job_params.get_destination_path()
        )

    def offline_to_online_ingestion(
        self, job_params: IngestionJobParameters
    ) -> IngestionJob:
        return DataprocIngestionJob(self.dataproc_submit(job_params))

    def stage_dataframe(
        self, df, event_timestamp_column: str, created_timestamp_column: str,
    ):
        raise NotImplementedError
