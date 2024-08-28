import copy


def get_cmd_prefix(image: str, n_jobs: int) -> str:
    return f"ibrun -n 1 apptainer run {image} --help && ibrun -n {n_jobs}"


def set_app_arg(job: dict, arg_pos: int, name: str, arg: str) -> dict:
    job2 = copy.deepcopy(job)
    job2.get("parameterSet").get("appArgs")[arg_pos] = {"name": name, "arg": arg}  # type: ignore
    return job2


def set_env_var(job: dict, arg_pos: int, key: str, value: str) -> dict:
    job2 = copy.deepcopy(job)
    job2.get("parameterSet").get("envVariables")[arg_pos] = {"key": key, "value": value}  # type: ignore
    return job2


def set_key_value(job: dict, key: str, value: int | str | None = None) -> None:
    if value:
        job[key] = value


def set_subscription_url(job: dict, arg: str) -> dict:
    job2 = copy.deepcopy(job)
    job2.get("subscriptions")[0].get("deliveryTargets")[0].update(  # type: ignore
        {"deliveryAddress": arg}
    )
    return job2
