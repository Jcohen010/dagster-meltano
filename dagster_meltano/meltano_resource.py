import asyncio
import json
import logging
import os
import yaml
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

from dagster import DagsterLogManager, resource, Field
from dagster_meltano.exceptions import MeltanoCommandError

from dagster_meltano.job import Job
from dagster_meltano.schedule import Schedule
from dagster_meltano.utils import Singleton
from dagster_shell import execute_shell_command

STDOUT = 1


class MeltanoResource(metaclass=Singleton):
    def __init__(
        self,
        project_dir: str = None,
        meltano_bin: Optional[str] = "meltano",
        retries: int = 0,
    ):
        self.project_dir = str(project_dir)
        self.meltano_bin = meltano_bin
        self.retries = retries

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
                f"Command '{command}' failed with exit code {exit_code}"
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
    
    async def load_taps_from_yaml(self) -> dict:
        """Parse meltano.yml in meltano project dir to 
        load tap names.
        
        """
        taps_dict = {'taps' : []}
        meltano_dot_yml_path = self.project_dir + "/meltano.yml"
        with open(meltano_dot_yml_path, 'r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        extractors = data['plugins']['extractors']
        for extractor in extractors:
            if extractor != None:
                extractor_object = {
                    "tap_name" : extractor['name'],
                    "streams" : []
                }
            taps_dict["taps"].append(extractor_object)
        
        return taps_dict
    
    async def fetch_stream_information(self, taps_dict: dict) -> dict:
        """Use tap names loaded from meltano.yml to 
        fetch selected streams from tep.properties.json
        file in .meltano/run/{tap_name} dir. 

        Only applicable to taps with the properites capability.
        
        """
        for i, tap_info in enumerate(taps_dict['taps']):
            print(tap_info['tap_name'])

            properties_json=f"{self.project_dir}/.meltano/run/{tap_info['tap_name']}/tap.properties.json"

            f = open(properties_json)

            data = json.load(f)

            for stream in data['streams']:
                if stream['selected'] == True:
                    taps_dict["taps"][i]['streams'].append(stream['tap_stream_id'])
        return taps_dict

    async def gather_meltano_yaml_information(self):
        taps, jobs, schedules = await asyncio.gather(
            self.fetch_stream_information(self.load_taps_from_yaml()),
            self.load_json_from_cli(["job", "list", "--format=json"]),
            self.load_json_from_cli(["schedule", "list", "--format=json"]),
        )

        return taps, jobs, schedules
    

    @cached_property
    def meltano_yaml(self) -> dict:
        """Asynchronously load the Meltano jobs and schedules.

        Returns:
            dict: The Meltano jobs and schedules.
        """
        taps, jobs, schedules = asyncio.run(self.gather_meltano_yaml_information())
        return {'taps' : taps['taps'], "jobs": jobs["jobs"], "schedules": schedules["schedules"]}

    @cached_property
    def meltano_jobs(self) -> List[Job]:
        meltano_job_list = self.meltano_yaml["jobs"]
        return [
            Job(
                meltano_job=meltano_job,
                retries=self.retries,
            )
            for meltano_job in meltano_job_list
        ]

    @cached_property
    def meltano_schedules(self) -> List[Schedule]:
        meltano_schedule_list = self.meltano_yaml["schedules"]["job"]
        schedule_list = [
            Schedule(meltano_schedule) for meltano_schedule in meltano_schedule_list
        ]
        return schedule_list

    @property
    def meltano_job_schedules(self) -> Dict[str, Schedule]:
        return {schedule.job_name: schedule for schedule in self.meltano_schedules}

    @property
    def jobs(self) -> List[dict]:
        for meltano_job in self.meltano_jobs:
            yield meltano_job.dagster_job

        for meltano_schedule in self.meltano_schedules:
            yield meltano_schedule.dagster_schedule


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
    meltano_resource.fetch_stream_information(meltano_resource.fetch_tap_information())