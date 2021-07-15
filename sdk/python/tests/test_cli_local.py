import tempfile
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent

import assertpy
import pytest

from feast.feature_store import FeatureStore
from tests.cli_utils import CliRunner
from tests.online_read_write_test import basic_rw_test


@pytest.mark.integration
def test_workflow() -> None:
    """
    Test running apply on a sample repo, and make sure the infra gets created.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)
        data_path = Path(data_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: {data_path / "registry.db"}
        provider: local
        online_store:
            path: {data_path / "online_store.db"}
        offline_store:
            type: bigquery
        """
            )
        )

        repo_example = repo_path / "example.py"
        repo_example.write_text(
            (Path(__file__).parent / "example_feature_repo_1.py").read_text()
        )

        result = runner.run(["apply"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)

        # entity & feature view list commands should succeed
        result = runner.run(["entities", "list"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)
        result = runner.run(["feature-views", "list"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)
        result = runner.run(["feature-services", "list"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)

        # entity & feature view describe commands should succeed when objects exist
        result = runner.run(["entities", "describe", "driver"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)
        result = runner.run(
            ["feature-views", "describe", "driver_locations"], cwd=repo_path
        )
        assertpy.assert_that(result.returncode).is_equal_to(0)
        result = runner.run(
            ["feature-services", "describe", "driver_locations_service"], cwd=repo_path
        )
        assertpy.assert_that(result.returncode).is_equal_to(0)

        # entity & feature view describe commands should fail when objects don't exist
        result = runner.run(["entities", "describe", "foo"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(1)
        result = runner.run(["feature-views", "describe", "foo"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(1)
        result = runner.run(["feature-services", "describe", "foo"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(1)

        # Doing another apply should be a no op, and should not cause errors
        result = runner.run(["apply"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)

        basic_rw_test(
            FeatureStore(repo_path=str(repo_path), config=None),
            view_name="driver_locations",
        )

        result = runner.run(["teardown"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)


@pytest.mark.integration
def test_non_local_feature_repo() -> None:
    """
    Test running apply on a sample repo, and make sure the infra gets created.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                """
        project: foo
        registry: data/registry.db
        provider: local
        online_store:
            path: data/online_store.db
        offline_store:
            type: bigquery
        """
            )
        )

        repo_example = repo_path / "example.py"
        repo_example.write_text(
            (Path(__file__).parent / "example_feature_repo_1.py").read_text()
        )

        result = runner.run(["apply"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)

        fs = FeatureStore(repo_path=str(repo_path))
        assertpy.assert_that(fs.list_feature_views()).is_length(3)

        result = runner.run(["teardown"], cwd=repo_path)
        assertpy.assert_that(result.returncode).is_equal_to(0)


@contextmanager
def setup_third_party_provider_repo(provider_name: str):
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: data/registry.db
        provider: {provider_name}
        online_store:
            path: data/online_store.db
            type: sqlite
        offline_store:
            type: file
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/provider.py"
        repo_example.write_text((Path(__file__).parent / "foo_provider.py").read_text())

        yield repo_path


def test_3rd_party_providers() -> None:
    """
    Test running apply on third party providers
    """
    runner = CliRunner()
    # Check with incorrect built-in provider name (no dots)
    with setup_third_party_provider_repo("feast123") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(b"Provider 'feast123' is not implemented")
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("feast_foo.Provider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import Provider module 'feast_foo'"
        )
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("foo.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import Provider 'FooProvider' from module 'foo'"
        )
    # Check with correct third-party provider name
    with setup_third_party_provider_repo("foo.provider.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(0)
