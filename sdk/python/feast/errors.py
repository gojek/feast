from typing import List, Set

from colorama import Fore, Style


class DataSourceNotFoundException(Exception):
    def __init__(self, path):
        super().__init__(
            f"Unable to find table at '{path}'. Please check that table exists."
        )


class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Entity {name} does not exist in project {project}")
        else:
            super().__init__(f"Entity {name} does not exist")


class FeatureServiceNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature service {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature service {name} does not exist")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Feature view {name} does not exist in project {project}")
        else:
            super().__init__(f"Feature view {name} does not exist")


class FeatureTableNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature table {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature table {name} does not exist")


class S3RegistryBucketNotExist(FeastObjectNotFoundException):
    def __init__(self, bucket):
        super().__init__(f"S3 bucket {bucket} for the Feast registry does not exist")


class S3RegistryBucketForbiddenAccess(FeastObjectNotFoundException):
    def __init__(self, bucket):
        super().__init__(f"S3 bucket {bucket} for the Feast registry can't be accessed")


class FeastProviderLoginError(Exception):
    """Error class that indicates a user has not authenticated with their provider."""


class FeastProviderNotImplementedError(Exception):
    def __init__(self, provider_name):
        super().__init__(f"Provider '{provider_name}' is not implemented")


class FeastModuleImportError(Exception):
    def __init__(self, module_name: str, module_type: str):
        super().__init__(f"Could not import {module_type} module '{module_name}'")


class FeastClassImportError(Exception):
    def __init__(self, module_name, class_name, class_type="provider"):
        super().__init__(
            f"Could not import {class_type} '{class_name}' from module '{module_name}'"
        )


class FeastExtrasDependencyImportError(Exception):
    def __init__(self, extras_type: str, nested_error: str):
        message = (
            nested_error
            + "\n"
            + f"You may need run {Style.BRIGHT + Fore.GREEN}pip install 'feast[{extras_type}]'{Style.RESET_ALL}"
        )
        super().__init__(message)


class FeastOfflineStoreUnsupportedDataSource(Exception):
    def __init__(self, offline_store_name: str, data_source_name: str):
        super().__init__(
            f"Offline Store '{offline_store_name}' does not support data source '{data_source_name}'"
        )


class FeatureNameCollisionError(Exception):
    def __init__(self, feature_refs_collisions: List[str], full_feature_names: bool):
        if full_feature_names:
            collisions = [ref.replace(":", "__") for ref in feature_refs_collisions]
            error_message = (
                "To resolve this collision, please ensure that the features in question "
                "have different names."
            )
        else:
            collisions = [ref.split(":")[1] for ref in feature_refs_collisions]
            error_message = (
                "To resolve this collision, either use the full feature name by setting "
                "'full_feature_names=True', or ensure that the features in question have different names."
            )

        feature_names = ", ".join(set(collisions))
        super().__init__(
            f"Duplicate features named {feature_names} found.\n{error_message}"
        )


class FeastOnlineStoreInvalidName(Exception):
    def __init__(self, online_store_class_name: str):
        super().__init__(
            f"Online Store Class '{online_store_class_name}' should end with the string `OnlineStore`.'"
        )


class FeastClassInvalidName(Exception):
    def __init__(self, class_name: str, class_type: str):
        super().__init__(
            f"Config Class '{class_name}' "
            f"should end with the string `{class_type}`.'"
        )


class FeastOnlineStoreUnsupportedDataSource(Exception):
    def __init__(self, online_store_name: str, data_source_name: str):
        super().__init__(
            f"Online Store '{online_store_name}' does not support data source '{data_source_name}'"
        )


class FeastEntityDFMissingColumnsError(Exception):
    def __init__(self, expected, missing):
        super().__init__(
            f"The entity dataframe you have provided must contain columns {expected}, "
            f"but {missing} were missing."
        )


class FeastJoinKeysDuringMaterialization(Exception):
    def __init__(
        self, source: str, join_key_columns: Set[str], source_columns: Set[str]
    ):
        super().__init__(
            f"The DataFrame from {source} being materialized must have at least {join_key_columns} columns present, "
            f"but these were missing: {join_key_columns - source_columns} "
        )


class RegistryInferenceFailure(Exception):
    def __init__(self, repo_obj_type: str, specific_issue: str):
        super().__init__(
            f"Inference to fill in missing information for {repo_obj_type} failed. {specific_issue}. "
            "Try filling the information explicitly."
        )


class BigQueryJobStillRunning(Exception):
    def __init__(self, job_id):
        super().__init__(f"The BigQuery job with ID '{job_id}' is still running.")


class BigQueryJobCancelled(Exception):
    def __init__(self, job_id):
        super().__init__(f"The BigQuery job with ID '{job_id}' was cancelled")


class RedshiftCredentialsError(Exception):
    def __init__(self):
        super().__init__("Redshift API failed due to incorrect credentials")


class RedshiftQueryError(Exception):
    def __init__(self, details):
        super().__init__(f"Redshift SQL Query failed to finish. Details: {details}")
