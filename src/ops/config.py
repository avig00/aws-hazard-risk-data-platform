import os

def get_env(name: str, default: str = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise ValueError(f"Missing required env var: {name}")
        return default
    return v

PLATFORM_S3_BUCKET = get_env("PLATFORM_S3_BUCKET")
OPS_PREFIX = get_env("OPS_PREFIX", "hazard/ops")

ATHENA_WORKGROUP = get_env("ATHENA_WORKGROUP")
ATHENA_RESULTS_S3 = get_env("ATHENA_RESULTS_S3")

ATHENA_DB_BRONZE = get_env("ATHENA_DB_BRONZE")
ATHENA_DB_SILVER = get_env("ATHENA_DB_SILVER")
ATHENA_DB_GOLD = get_env("ATHENA_DB_GOLD")
