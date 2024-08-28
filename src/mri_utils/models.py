import abc
import dataclasses
import functools
import io
import math
import os
import typing
from pathlib import Path

import ibis
import pandas as pd
from ibis.expr.types.relations import Table
from tapipy import errors, util, actors
from tapipy.tapis import Tapis, TapisResult


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


@dataclasses.dataclass
class Reactor:
    job_name: str
    ILOG: Path
    JOB: Path
    N_SUBS_PER_NODE: int
    N_SEC_TO_COPY_ONE_SUB: int
    MAXJOBS: int
    FAILUREBOT_ADDRESS_SECRET_KEY: str
    FAILUREBOT_ADDRESS_SECRET_NAME: str

    @functools.cached_property
    def context(self) -> Context:
        return actors.get_context()  # type: ignore

    @functools.cached_property
    def ilog(self) -> Table:
        ilog: bytes = self.client.files.getContents(  # type: ignore
            systemId="secure.corral",
            path=self.context.message_dict.get("ILOG", str(self.ILOG)),
        )
        return ibis.memtable(
            pd.read_csv(
                io.BytesIO(ilog),
                na_values=["na", ""],
                dtype={"subject_id": str},
            )
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

    def get_node_count(self, n_jobs: int) -> int:
        return math.ceil(n_jobs / self.N_SUBS_PER_NODE)

    @abc.abstractmethod
    def get_runlist(self) -> list[tuple[str, str]]:
        pass

    @abc.abstractmethod
    def submit(self) -> None:
        pass
