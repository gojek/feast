# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from datetime import datetime
from pathlib import Path
from typing import List

import click
import pkg_resources
import yaml

from feast import utils
from feast.errors import FeastObjectNotFoundException, FeastProviderLoginError
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config
from feast.repo_operations import (
    apply_total,
    cli_check_repo,
    generate_project_name,
    init_repo,
    registry_dump,
    teardown,
)

_logger = logging.getLogger(__name__)
DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"


class NoOptionDefaultFormat(click.Command):
    def format_options(self, ctx: click.Context, formatter: click.HelpFormatter):
        """Writes all the options into the formatter if they exist."""
        opts = []
        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                opts.append(rv)
        if opts:
            with formatter.section("Options(No current command options)"):
                formatter.write_dl(opts)


@click.group()
@click.option(
    "--chdir",
    "-c",
    help="Switch to a different feature repository directory before executing the given subcommand.",
)
@click.pass_context
def cli(ctx: click.Context, chdir: str):
    """
    Feast CLI

    For more information, see our public docs at https://docs.feast.dev/

    For any questions, you can reach us at https://slack.feast.dev/
    """
    ctx.ensure_object(dict)
    ctx.obj["CHDIR"] = Path.cwd() if chdir is None else Path(chdir).absolute()
    pass


@cli.command()
def version():
    """
    Display Feast SDK version
    """
    print(f'Feast SDK Version: "{pkg_resources.get_distribution("feast")}"')


@cli.group(name="entities")
def entities_cmd():
    """
    Access entities
    """
    pass


@entities_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def entity_describe(ctx: click.Context, name: str):
    """
    Describe an entity
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    try:
        entity = store.get_entity(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(entity)), default_flow_style=False, sort_keys=False
        )
    )


@entities_cmd.command(name="list")
@click.pass_context
def entity_list(ctx: click.Context):
    """
    List all entities
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    table = []
    for entity in store.list_entities():
        table.append([entity.name, entity.description, entity.value_type])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "DESCRIPTION", "TYPE"], tablefmt="plain"))


@cli.group(name="feature-services")
def feature_services_cmd():
    """
    Access feature services
    """
    pass


@feature_services_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_service_describe(ctx: click.Context, name: str):
    """
    Describe a feature service
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    try:
        feature_service = store.get_feature_service(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(feature_service)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


@feature_services_cmd.command(name="list")
@click.pass_context
def feature_service_list(ctx: click.Context):
    """
    List all feature services
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    feature_services = []
    for feature_service in store.list_feature_services():
        feature_names = []
        for projection in feature_service.features:
            feature_names.extend(
                [f"{projection.name}:{feature.name}" for feature in projection.features]
            )
        feature_services.append([feature_service.name, ", ".join(feature_names)])

    from tabulate import tabulate

    print(tabulate(feature_services, headers=["NAME", "FEATURES"], tablefmt="plain"))


@cli.group(name="feature-views")
def feature_views_cmd():
    """
    Access feature views
    """
    pass


@feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_describe(ctx: click.Context, name: str):
    """
    Describe a feature view
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    try:
        feature_view = store.get_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    print(
        yaml.dump(
            yaml.safe_load(str(feature_view)), default_flow_style=False, sort_keys=False
        )
    )


@feature_views_cmd.command(name="list")
@click.pass_context
def feature_view_list(ctx: click.Context):
    """
    List all feature views
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    table = []
    for feature_view in store.list_feature_views():
        table.append([feature_view.name, feature_view.entities])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME", "ENTITIES"], tablefmt="plain"))


@cli.command("apply", cls=NoOptionDefaultFormat)
@click.pass_context
def apply_total_command(ctx: click.Context):
    """
    Create or update a feature store deployment
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)
    try:
        apply_total(repo_config, repo)
    except FeastProviderLoginError as e:
        print(str(e))


@cli.command("teardown", cls=NoOptionDefaultFormat)
@click.pass_context
def teardown_command(ctx: click.Context):
    """
    Tear down deployed feature store infrastructure
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)

    teardown(repo_config, repo)


@cli.command("registry-dump")
@click.pass_context
def registry_dump_command(ctx: click.Context):
    """
    Print contents of the metadata registry
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)

    registry_dump(repo_config, repo_path=repo)


@cli.command("materialize")
@click.argument("start_ts")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to materialize", multiple=True,
)
@click.pass_context
def materialize_command(
    ctx: click.Context, start_ts: str, end_ts: str, views: List[str]
):
    """
    Run a (non-incremental) materialization job to ingest data into the online store. Feast
    will read all data between START_TS and END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be materialized.

    START_TS and END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    store.materialize(
        feature_views=None if not views else views,
        start_date=utils.make_tzaware(datetime.fromisoformat(start_ts)),
        end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
    )


@cli.command("materialize-incremental")
@click.argument("end_ts")
@click.option(
    "--views", "-v", help="Feature views to incrementally materialize", multiple=True,
)
@click.pass_context
def materialize_incremental_command(ctx: click.Context, end_ts: str, views: List[str]):
    """
    Run an incremental materialization job to ingest new data into the online store. Feast will read
    all data from the previously ingested point to END_TS from the offline store and write it to the
    online store. If you don't specify feature view names using --views, all registered Feature
    Views will be incrementally materialized.

    END_TS should be in ISO 8601 format, e.g. '2021-07-16T19:20:01'
    """
    repo = ctx.obj["CHDIR"]
    cli_check_repo(repo)
    store = FeatureStore(repo_path=str(repo))
    store.materialize_incremental(
        feature_views=None if not views else views,
        end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
    )


@cli.command("init")
@click.argument("PROJECT_DIRECTORY", required=False)
@click.option(
    "--minimal", "-m", is_flag=True, help="Create an empty project repository"
)
@click.option(
    "--template",
    "-t",
    type=click.Choice(["local", "gcp", "aws"], case_sensitive=False),
    help="Specify a template for the created project",
    default="local",
)
def init_command(project_directory, minimal: bool, template: str):
    """Create a new Feast repository"""
    if not project_directory:
        project_directory = generate_project_name()

    if minimal:
        template = "minimal"

    init_repo(project_directory, template)


if __name__ == "__main__":
    cli()
