import asyncio
import json
import logging
import os
import yaml
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

from dagster import DagsterLogManager, resource, Field, get_dagster_logger
from dagster_meltano.exceptions import MeltanoCommandError

from dagster_meltano.asset import Asset
from dagster_meltano.job import Job
from dagster_meltano.schedule import Schedule
from dagster_meltano.utils import Singleton
from dagster_shell import execute_shell_command

STDOUT = 1

dagster_logger = get_dagster_logger()


class MeltanoResource(metaclass=Singleton):
    def __init__(
        self,
        project_dir: str = None,
        meltano_bin: Optional[str] = "meltano",
        retries: int = 0,
        target: str = None
    ):
        self.project_dir = str(project_dir)
        self.meltano_bin = meltano_bin
        self.retries = retries
        self.target = str(target)

    @property
    def default_env(self) -> Dict[str, str]:
        """The default environment to use when running Meltano commands.

        Returns:
            Dict[str, str]: The environment variables.
        """
        return {
            "MELTANO_CLI_LOG_CONFIG": str(Path(__file__).parent / "logging.yaml"),
            "DBT_USE_COLORS": "false",
            "NO_COLOR": "1",
            **os.environ.copy(),
        }

    def execute_command(
        self,
        command: str,
        env: Dict[str, str],
        logger: Union[logging.Logger, DagsterLogManager] = logging.Logger,
    ) -> str:
        """Execute a Meltano command.

        Args:
            context (OpExecutionContext): The Dagster execution context.
            command (str): The Meltano command to execute.
            env (Dict[str, str]): The environment variables to inject when executing the command.

        Returns:
            str: The output of the command.
        """
        output, exit_code = execute_shell_command(
            f"meltano {command}",
            env={**self.default_env, **env},
            output_logging="STREAM",
            log=logger,
            cwd=self.project_dir,
        )

        if exit_code != 0:
            raise MeltanoCommandError(
                f"Command '{command}' failed with exit code {exit_code}: \n {output}"
            )

        return output

    async def load_json_from_cli(self, command: List[str]) -> dict:
        """Use the Meltano CLI to load JSON data.
        Use asyncio to run multiple commands concurrently.

        Args:
            command (List[str]): The Meltano command to execute.

        Returns:
            dict: The processed JSON data.
        """
        # Create the subprocess, redirect the standard output into a pipe
        proc = await asyncio.create_subprocess_exec(
            "meltano",
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_dir,
        )

        # Wait for the subprocess to finish
        stdout, stderr = await proc.communicate()

        # Try to load the output as JSON
        try:
            return json.loads(stdout)
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Could not process json: {stdout} {stderr}")
        
    async def load_discoverable_streams_from_cli(self, command: List[str]) -> dict:
        """Use the Meltano CLI to load JSON data.
        Use asyncio to run multiple commands concurrently.

        Args:
            command (List[str]): The Meltano command to execute.

        Returns:
            dict: The processed JSON data.
        """
        # Create the subprocess, redirect the standard output into a pipe
        proc = await asyncio.create_subprocess_exec(
            "meltano",
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_dir,
        )

        # Wait for the subprocess to finish
        stdout, stderr = await proc.communicate()

        # Try to load the output as JSON

        components = str(stdout).split("\n")

        for component in components:
            # if '[select ]' in component:
            print(component)

    # Not sure if this should be async, still thinking on it.
    async def compile_manifest(self) -> None:
        """
        Run `meltano compile` command to generate 
        meltano_manifest.json file.

        """

        # Create the subprocess.
        await asyncio.create_subprocess_exec(
            "meltano",
            "compile",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_dir,
        )

        
    async def load_taps_streams_from_manifest(self) -> dict:
        """
        Parse meltano-manifest.json in .meltano project dir to 
        load tap and stream names.

        Returns:
            dict with meltano taps and streams.
        
        """
        taps_dict = {'taps' : []}
        meltano_manifest_json_path = self.project_dir + "/.meltano/manifests/meltano-manifest.json"

        await self.compile_manifest()

        with open(meltano_manifest_json_path, 'r') as f:
            data = json.load(f)
        extractors = data['plugins']['extractors']

        for i, extractor in enumerate(extractors):
            dagster_logger.info(f'{extractor=}')
            taps_dict['taps'].append(
                {
                    'tap_name': extractor['name'], 
                    'streams': []
                }
                    )
        
            if 'streams' in extractor['config']:
                for stream in extractor['config']['streams']:
                    taps_dict['taps'][i]['streams'].append(stream['stream_name'])

            elif extractor['select']:
                for select in extractor['select']:
                    taps_dict['taps'][i]['streams'].append(select)

        return taps_dict
        

    async def gather_meltano_dict_information(self):
        taps, jobs, schedules = await asyncio.gather(
            self.load_taps_streams_from_manifest(),
            self.load_json_from_cli(["job", "list", "--format=json"]),
            self.load_json_from_cli(["schedule", "list", "--format=json"]),
        )

        return taps, jobs, schedules
    

    @cached_property
    def meltano_dict(self) -> dict:
        """Asynchronously load the Meltano taps, streams, jobs and schedules.

        Returns:
            dict: The Meltano taps, streams, jobs and schedules.
        """
        taps, jobs, schedules = asyncio.get_event_loop().run_until_complete(self.gather_meltano_dict_information())
        return {'taps' : taps['taps'], "jobs": jobs["jobs"], "schedules": schedules["schedules"]}

    @cached_property
    def meltano_jobs(self) -> List[Job]:
        meltano_job_list = self.meltano_dict["jobs"]
        return [
            Job(
                meltano_job=meltano_job,
                retries=self.retries,
            )
            for meltano_job in meltano_job_list
        ]

    @cached_property
    def meltano_schedules(self) -> List[Schedule]:
        meltano_schedule_list = self.meltano_dict["schedules"]["job"]
        schedule_list = [
            Schedule(meltano_schedule) for meltano_schedule in meltano_schedule_list
        ]
        return schedule_list
    
    @cached_property
    def meltano_tap_streams(self) -> List[Asset]:
        tapstreams_list = [
            Asset(meltano_tapstream) for meltano_tapstream in self.meltano_dict["taps"]
        ]
        return tapstreams_list

    @property
    def meltano_job_schedules(self) -> Dict[str, Schedule]:
        return {schedule.job_name: schedule for schedule in self.meltano_schedules}

    @property
    def jobs(self) -> List[dict]:
        for meltano_job in self.meltano_jobs:
            yield meltano_job.dagster_job

        for meltano_schedule in self.meltano_schedules:
            yield meltano_schedule.dagster_schedule

    @property
    def assets(self) -> List[dict]:
        asset_list = []
        for tap in self.meltano_tap_streams:
            for stream in tap.dagster_asset:
                asset_list.append(stream)

        return asset_list

@resource(
    description="A resource that corresponds to a Meltano project.",
    config_schema={
        "project_dir": Field(
            str,
            description="The path to the Meltano project.",
            default_value=os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
            is_required=False,
        ),
        "retries": Field(
            int,
            description="The number of times to retry a failed job.",
            default_value=0,
            is_required=False,
        ),
    },
)
def meltano_resource(init_context):
    project_dir = init_context.resource_config["project_dir"]
    retries = init_context.resource_config["retries"]

    return MeltanoResource(
        project_dir=project_dir,
        retries=retries,
    )


if __name__ == "__main__":
    meltano_resource = MeltanoResource("./meltano_project")
    # asyncio.run(meltano_resource.compile_manifest())
    print(meltano_resource.meltano_tap_streams)
    # asyncio.run(meltano_resource.load_discoverable_streams_from_cli(["select", "tap-mysql", "--list", "all"]))