import json
from typing import Any, Mapping, Sequence

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from gws_pipeline.defs.analytics.resources import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path, name="dbt_models")
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


def build_dbt_sources(
    manifest: Mapping[str, Any], dagster_dbt_translator: DagsterDbtTranslator
) -> Sequence[dg.SourceAsset]:
    return [
        dg.SourceAsset(
            key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
            group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
        )
        for dbt_resource_props in manifest["sources"].values()
    ]


translator = DagsterDbtTranslator()

with open(dbt_project.manifest_path, "r") as f:
    manifest = json.load(f)
dbt_source_assets = build_dbt_sources(manifest=manifest, dagster_dbt_translator=translator)
