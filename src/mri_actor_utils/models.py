import abc
import dataclasses
import functools
import io
import logging
import json
import math
import os
import typing
from pathlib import Path

import polars as pl
import pydantic
from tapipy import errors, util, actors
from tapipy.tapis import Tapis, TapisResult

from mri_actor_utils import jobs, config


@dataclasses.dataclass
class Context(util.AttrDict):
    raw_message: str
    content_type: str
    actor_repo: str
    actor_name: str
    actor_id: str
    actor_dbid: str
    execution_id: str
    worker_id: str
    username: str
    state: str
    raw_message_parse_log: str
    message_dict: dict[str, typing.Any]


class Reactor(pydantic.BaseModel):
    job_name: str
    JOB: Path
    N_SUBS_PER_NODE: int
    N_SEC_TO_COPY_ONE_SUB: int
    MAXJOBS: int
    ILOG: Path = config.ILOG
    N_SUBMISSIONS: int = 1
    FAILUREBOT_ADDRESS_SECRET_KEY: str = config.FAILUREBOT_ADDRESS_SECRET_KEY
    FAILUREBOT_ADDRESS_SECRET_NAME: str = config.FAILUREBOT_ADDRESS_SECRET_NAME

    _job: jobs.ReqSubmitJob | None = None

    @functools.cached_property
    def context(self) -> Context:
        return actors.get_context()  # type: ignore

    @functools.cached_property
    def ilog(self) -> pl.DataFrame:
        ilog: bytes = self.client.files.getContents(  # type: ignore
            systemId="secure.ls6",
            path=self.context.message_dict.get("ILOG", str(self.ILOG)),
        )
        return pl.read_csv(
            io.BytesIO(ilog),
            null_values=["na", ""],
            schema_overrides={"subject_id": pl.Utf8},
        )

    @functools.cached_property
    def client(self) -> Tapis:
        """
        Returns a pre-authenticated Tapis client using the abaco environment variables.
        """
        # if we have an access token, use that:
        if token := os.environ.get("_abaco_access_token"):
            tp = Tapis(
                base_url=os.environ.get("_abaco_api_server", default="").strip(
                    "/"
                ),
                access_token=token,
            )  # type: ignore
        elif server := os.environ.get("_abaco_api_server"):
            # otherwise, create a client with a fake JWT. this will only work if the actor
            # supplies its own token to itself via a config object or the message, etc.
            tp = Tapis(base_url=server.strip("/"), jwt="123")  # type: ignore
        else:
            raise errors.BaseTapyException(
                "Unable to instantiate a Tapis client: no token found."
            )
        return tp

    @property
    def failurebot_url(self) -> str:
        token: TapisResult = self.client.sk.readSecret(  # type: ignore
            secretType="user",
            secretName=self.FAILUREBOT_ADDRESS_SECRET_NAME,
            tenant=os.environ.get("_abaco_api_server")
            .split(".")[0]  # type: ignore
            .split("/")[-1],
            user=self.client.actors.get_actor(  # type: ignore
                actor_id=os.environ.get("_abaco_actor_id")
            ).owner,
        )
        url: str | None = token.get("secretMap").get(self.FAILUREBOT_ADDRESS_SECRET_KEY)  # type: ignore
        if url is None:
            msg = f"unable to find {self.FAILUREBOT_ADDRESS_SECRET_KEY} in secretMap"
            raise AssertionError(msg)

        return url

    @property
    def job(self) -> jobs.ReqSubmitJob:
        if self._job is None:
            with open(self.JOB) as f:
                self._job = jobs.ReqSubmitJob(**json.load(f))
        assert isinstance(self._job, jobs.ReqSubmitJob)
        return self._job

    @property
    def parameter_set(self) -> jobs.JobParameterSet:
        if self.job.parameterSet is None:
            parameter_set = jobs.JobParameterSet()
        else:
            parameter_set = self.job.parameterSet
        return parameter_set

    @property
    def container_image(self) -> str:
        app: TapisResult = self.client.apps.getApp(  # type: ignore
            appId=self.job.appId, appVersion=self.job.appVersion
        )
        image = app.get("containerImage")
        if image is None:
            msg = f"Did not find image for app: {app}"
            raise AssertionError(msg)
        return image

    @property
    def maxjobs(self) -> int:
        maxjobs = self.context.message_dict.get("MAXJOBS", self.MAXJOBS)
        assert isinstance(maxjobs, int)
        return maxjobs

    @property
    def n_submissions(self) -> int:
        n_submissions = self.context.message_dict.get(
            "N_SUBMISSIONS", self.N_SUBMISSIONS
        )
        assert isinstance(n_submissions, int)
        return n_submissions

    def set_cmd_prefix(self, image: str, n_jobs: int) -> None:
        self.job.cmdPrefix = (
            f"ibrun -n 1 apptainer run {image} --help && ibrun -n {n_jobs}"
        )

    def set_app_arg(self, name: str, value: str) -> None:
        app_args = self.parameter_set.appArgs
        new_arg = jobs.JobArgSpec(name=name, arg=value)
        if app_args is None:
            app_args = [new_arg]
        elif any(name == arg.name for arg in app_args):
            for arg in app_args:
                if name == arg.name:
                    arg.arg = value
        else:
            app_args.append(new_arg)

    def set_env_var(self, key: str, value: str) -> None:
        env_variables = self.parameter_set.envVariables
        new_variable = jobs.KeyValuePair(key=key, value=value)
        if env_variables is None:
            env_variables = [new_variable]
        elif any(key == var.key for var in env_variables):
            for var in env_variables:
                if key == var.key:
                    var.value = value
        else:
            env_variables.append(new_variable)

    def set_subscription_url(self, url: str) -> None:
        if self.job.subscriptions is None:
            self.job.subscriptions = [
                jobs.ReqSubscribe(
                    enabled=True,
                    deliveryTargets=[
                        jobs.NotifDeliveryTarget(
                            deliveryMethod="WEBHOOK", deliveryAddress=url
                        )
                    ],
                )
            ]
        else:
            target = self.job.subscriptions[0]
            target.deliveryTargets = [
                jobs.NotifDeliveryTarget(
                    deliveryMethod="WEBHOOK", deliveryAddress=url
                )
            ]

    def get_node_count(self, n_jobs: int) -> int:
        return math.ceil(n_jobs / self.N_SUBS_PER_NODE)

    def set_common(self, n_jobs: int) -> None:
        self.set_env_var(
            key="MIN_ARCHIVE_DURATION",
            value=str(n_jobs * self.N_SEC_TO_COPY_ONE_SUB),
        )
        if max_minutes := self.context.message_dict.get("maxMinutes"):
            self.job.maxMinutes = max_minutes

        self.set_cmd_prefix(image=self.container_image, n_jobs=n_jobs)

        n_nodes = self.get_node_count(n_jobs)

        # corresponds to SBATCH option -N,--nodes, SLURM_JOB_NUM_NODES
        self.job.nodeCount = n_nodes

        # corresponds to SBATCH option -n,--ntask, SLURM_NPROCS, SLURM_NTASKS
        # all nodes will have all cores available, but this needs to be set for ibrun
        self.job.coresPerNode = self.N_SUBS_PER_NODE

        if self.context.message_dict.get("SKIP_FAILUREBOT", False):
            self.job.subscriptions = None
        else:
            self.set_subscription_url(url=self.failurebot_url)

        if FAILURE_LOG_DST := self.context.message_dict.get("FAILURE_LOG_DST"):
            self.set_env_var(key="FAILURE_LOG_DST", value=FAILURE_LOG_DST)

    def submit(self) -> None:
        print(
            self.job.model_dump_json(
                indent=4, exclude_unset=True, exclude_none=True
            )
        )

        try:
            submitted = self.client.jobs.submitJob(  # type: ignore
                **self.job.model_dump(exclude_unset=True, exclude_none=True)
            )
            print(submitted.uuid)
        except Exception:
            logging.exception("encountered while trying to submit job")

    @abc.abstractmethod
    def get_runlist(self) -> list[tuple[str, ...]]:
        pass

    @abc.abstractmethod
    def parse_inputs(self) -> typing.Self:
        pass
