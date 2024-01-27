from dagster import Definitions, load_assets_from_modules

import os

from dagster import Definitions, AssetOut, define_asset_job, AssetSelection, DefaultScheduleStatus, OpExecutionContext, ScheduleDefinition, ConfigurableResource, multi_asset
from dagster_meltano import meltano_resource, meltano_run_op, load_jobs_from_meltano_project, load_assets_from_meltano_project, MeltanoResource

meltano_resource = MeltanoResource(project_dir='../..', target='target-duckdb')

# [print(meltano_tapstream) for meltano_tapstream in meltano_resource.meltano_yaml["taps"]]

defs = Definitions(
    assets=(meltano_resource.assets),
    resources={
        "meltano": meltano_resource,
    },
)


### TODO: investigate changing compute op to 
### DONE: issue with op definition overwriting compute every iteration in streams
    ### Used context.op.name 
### TODO: change asset name to allign meltano target naming convention
### DONE: change compute operation to actually run individual tapstream
### TODO: update read me about usage (atm selected streams cannot be defined as globs), limitations, meltano resource definion.
### TODO: test w/ database target
### TODO: retest w/ database tap
### TODO: write unit tests
### TODO: remove boilerplate meltano target in asset definition 
### TODO: add asset definition metadata

