from dagster import AssetsDefinition, multi_asset, AssetOut, OpExecutionContext, ConfigurableResource, asset

from dagster_meltano.utils import generate_dagster_name

from dagster_meltano import meltano_resource


class Asset:
    def __init__(self, meltano_tapstream: dict) -> None:
        self.tap_name = meltano_tapstream['tap_name']
        self.streams = meltano_tapstream['streams']

    @property
    def dagster_tap_name(self) -> str:
        return generate_dagster_name(self.tap_name)
    
    @property
    def dagster_streams(self) -> str:
        dagster_stream_name_list = []
        for stream in self.streams:
            dagster_safe_name = generate_dagster_name(stream)
            dagster_stream_name_list.append(dagster_safe_name)
        return dagster_stream_name_list    

    @property
    def dagster_asset(self) -> AssetsDefinition:
        asset_list = []
        for i, stream in enumerate(self.dagster_streams):
            @asset(
                
                ### TODO: change asset name to allign meltano target naming convention with dbt source naming convention    
                name=stream,
                compute_kind="meltano",
                group_name=self.dagster_tap_name,
            )
            def compute(context: OpExecutionContext, meltano: ConfigurableResource):
                command = f"el {self.tap_name} {meltano.target} --select {context.op.name}.*"
                meltano.execute_command(f"{command}", dict(), context.log)
                return tuple([None for _ in context.selected_output_names])
            asset_list.append(compute)
        return asset_list