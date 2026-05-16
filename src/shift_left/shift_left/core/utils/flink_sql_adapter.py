"""
Copyright 2024-2025 Confluent, Inc.

Flink SQL via confluent-sql, preserving legacy bootstrap-derived endpoints; REST PATCH
for stop/start remains on ConfluentCloudClient (requests).
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any

import confluent_sql
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from shift_left.core.utils.app_config import logger
from shift_left.core.utils.ccloud_client import ConfluentCloudClient, VersionInfo


def infer_flink_sql_endpoint(config: dict) -> str:
    """Return Flink SQL REST host base URL (scheme + host, no /sql/v1 path)."""

    endpoint = (
        os.getenv("SL_FLINK_SQL_ENDPOINT")
        or config.get("flink", {}).get("sql_endpoint")
        or config.get("flink", {}).get("endpoint")
    )
    if endpoint:
        return str(endpoint).rstrip("/")

    tmp = ConfluentCloudClient(config)
    if tmp.base_url:
        return f"https://flink.{tmp.base_url}".rstrip("/")

    cc = config["confluent_cloud"]
    provider = cc.get("cloud_provider") or cc.get("provider") or "aws"
    region = cc.get("cloud_region") or cc.get("region") or ""
    return f"https://flink.{region}.{provider}.confluent.cloud".rstrip("/")


def http_user_agent() -> str:
    return f"python-shift-left-utils/{VersionInfo.get_version()}"[:100]


def sanitize_statement_properties(properties: dict[str, Any] | None) -> dict[str, Any]:
    reserved = frozenset(
        {"sql.current-catalog", "sql.current-database", "sql.snapshot.mode"}
    )
    if not properties:
        return {}
    return {k: v for k, v in properties.items() if k not in reserved}


@contextmanager
def flink_connection(config: dict, *, compute_pool_id: str | None = None):
    cc = config["confluent_cloud"]
    flink = config["flink"]
    pk = os.getenv("SL_FLINK_API_KEY") or flink["api_key"]
    secret = os.getenv("SL_FLINK_API_SECRET") or flink["api_secret"]

    catalog = config.get("flink", {}).get("catalog_name")
    env_id = cc.get("environment_id")
    if catalog and env_id and str(catalog) != str(env_id):
        logger.warning(
            "confluent-sql driver sets catalog from environment_id; flink.catalog_name=%s differs.",
            catalog,
        )

    conn = confluent_sql.connect(
        flink_api_key=pk,
        flink_api_secret=secret,
        environment_id=cc["environment_id"],
        compute_pool_id=compute_pool_id or flink["compute_pool_id"],
        organization_id=cc["organization_id"],
        endpoint=infer_flink_sql_endpoint(config),
        database=flink.get("database_name"),
        http_user_agent=http_user_agent(),
    )
    try:
        yield conn
    finally:
        conn.close()


def _classify_execution(sql: str) -> str:
    s = sql.strip().lower()
    while s.startswith("--"):
        nl = s.find("\n")
        if nl == -1:
            s = ""
            break
        s = s[nl + 1 :].lstrip()

    if s.startswith(("select ", "show ", "describe ", "explain ")):
        return "snapshot_query"
    if "insert into " in s or "create materialized " in s:
        return "streaming_ddl"
    if s.startswith("create table ") and " as select " in s:
        return "streaming_ddl"
    return "snapshot_ddl"


def driver_statement_to_pydantic(conn, drv_stmt) -> Any:
    from shift_left.core.models.flink_statement_model import Statement as PStatement

    spec = dict(drv_stmt.spec)
    if spec.get("principal") in (None, ""):
        spec["principal"] = ""

    merged = {
        "name": drv_stmt.name,
        "spec": spec,
        "status": dict(drv_stmt.status),
        "metadata": dict(drv_stmt.metadata),
        "organization_id": conn.organization_id,
        "environment_id": conn.environment_id,
    }
    return PStatement.model_validate(merged)


def wait_until_statement_not_pending_via_driver(
    config: dict,
    *,
    compute_pool_id: str,
    statement_name: str,
    start_time: float,
) -> Any:
    """Poll Flink statement phase until leaving PENDING (legacy wait_response)."""

    from shift_left.core.models.flink_statement_model import Statement as PStatement

    timer = float(config["flink"].get("poll_timer", 10))
    pending_counter = 0
    error_counter = 0

    while True:
        try:
            with flink_connection(config, compute_pool_id=compute_pool_id) as conn:
                drv = conn.get_statement(statement_name)
                if drv.phase.value not in ("PENDING",):
                    ps = driver_statement_to_pydantic(conn, drv)
                    ps.execution_time = time.perf_counter() - start_time  # type: ignore[attr-defined]
                    ps.loop_counter = pending_counter  # type: ignore[attr-defined]
                    return ps
        except OperationalError:
            raise
        except Exception as e:
            error_counter += 1
            if error_counter > 5:
                raise Exception(f"wait failed for {statement_name}: {e}") from e
            logger.warning("wait poll error for %s: %s", statement_name, e)
            time.sleep(timer)

        time.sleep(timer)
        pending_counter += 1
        if pending_counter % 3 == 0:
            timer += 10
            print(f"Wait {statement_name} deployment, increase poll timer to {timer} seconds")

        if pending_counter >= 23:
            err = PStatement.model_validate(
                {
                    "name": statement_name,
                    "spec": {},
                    "status": {"phase": "FAILED", "detail": "Timed out waiting for completion"},
                    "loop_counter": pending_counter,
                    "execution_time": time.perf_counter() - start_time,
                    "environment_id": config["confluent_cloud"]["environment_id"],
                    "organization_id": config["confluent_cloud"]["organization_id"],
                },
            )
            raise Exception(err.model_dump_json(indent=3))


def _operational_error_detail(exc: OperationalError) -> str:
    """Stable user-facing text for confluent-sql OperationalError (message + HTTP code when set)."""
    msg = str(exc)
    if exc.http_status_code is not None:
        return f"{msg} (HTTP {exc.http_status_code})"
    return msg


def _submit_inner(
    conn,
    config: dict,
    *,
    compute_pool_id: str,
    statement_name: str,
    sql_content: str,
    user_props: dict[str, Any],
    timeout_sec: int,
) -> Any:
    from shift_left.core.models.flink_statement_model import Statement as PStatement

    kind = _classify_execution(sql_content)
    start = time.perf_counter()

    if kind == "snapshot_query":
        with conn.closing_cursor(mode=ExecutionMode.SNAPSHOT) as cur:
            cur.execute(
                sql_content,
                timeout=timeout_sec,
                statement_name=statement_name,
                properties=user_props or None,
                compute_pool_id=compute_pool_id,
            )
            drv = cur.statement
    elif kind == "streaming_ddl":
        drv = conn.execute_streaming_ddl(
            sql_content,
            timeout=timeout_sec,
            statement_name=statement_name,
            properties=user_props or None,
            compute_pool_id=compute_pool_id,
        )
    else:
        drv = conn.execute_snapshot_ddl(
            sql_content,
            timeout=timeout_sec,
            statement_name=statement_name,
            properties=user_props or None,
            compute_pool_id=compute_pool_id,
        )

    phase = drv.phase.value
    if phase == "PENDING":
        return wait_until_statement_not_pending_via_driver(
            config,
            compute_pool_id=compute_pool_id,
            statement_name=statement_name,
            start_time=start,
        )

    ps = driver_statement_to_pydantic(conn, drv)
    ps.execution_time = time.perf_counter() - start  # type: ignore[attr-defined]
    return ps


def submit_flink_statement(
    config: dict,
    *,
    compute_pool_id: str,
    statement_name: str,
    sql_content: str,
    properties: dict[str, Any],
    stopped: bool = False,
):
    from shift_left.core.models.flink_statement_model import ErrorData, StatementError

    if stopped:
        return StatementError(
            errors=[
                ErrorData(
                    id=statement_name,
                    status="FAILED",
                    detail="stopped=true is not supported via confluent-sql path",
                ),
            ],
        )

    user_props = sanitize_statement_properties(properties)
    timeout_sec = int(config["flink"].get("submission_timeout_seconds", 3000))

    with flink_connection(config, compute_pool_id=compute_pool_id) as conn:
        try:
            return _submit_inner(
                conn,
                config,
                compute_pool_id=compute_pool_id,
                statement_name=statement_name,
                sql_content=sql_content,
                user_props=user_props,
                timeout_sec=timeout_sec,
            )
        except OperationalError as e:
            if e.http_status_code == 409:
                try:
                    conn.delete_statement(statement_name)
                except Exception:
                    pass
                try:
                    return _submit_inner(
                        conn,
                        config,
                        compute_pool_id=compute_pool_id,
                        statement_name=statement_name,
                        sql_content=sql_content,
                        user_props=user_props,
                        timeout_sec=timeout_sec,
                    )
                except OperationalError as e2:
                    return StatementError(
                        errors=[
                            ErrorData(
                                id=statement_name,
                                status="FAILED",
                                detail=_operational_error_detail(e2),
                            ),
                        ],
                    )

            detail = _operational_error_detail(e)
            logger.error(f"OperationalError submitting {statement_name}: {detail}")
            return StatementError(
                errors=[ErrorData(id=statement_name, status="FAILED", detail=detail)],
            )
        except Exception as e:
            logger.error(f"Error submitting statement {statement_name}: {e}")
            return StatementError(
                errors=[ErrorData(id=statement_name, status="FAILED", detail=str(e))],
            )


def get_flink_statement_optional(config: dict, statement_name: str):
    try:
        with flink_connection(config) as conn:
            drv = conn.get_statement(statement_name)
            return driver_statement_to_pydantic(conn, drv)
    except StatementNotFoundError:
        logger.warning("Statement %s not found", statement_name)
        return None


def delete_flink_statement(config: dict, statement_name: str) -> str:
    with flink_connection(config) as conn:
        conn.delete_statement(statement_name)
        counter = 0
        timer = float(config["flink"].get("poll_timer", 10))
        while True:
            try:
                conn.get_statement(statement_name)
            except StatementNotFoundError:
                logger.info("Statement %s deleted", statement_name)
                return "deleted"
            counter += 1
            if counter == 6:
                timer = 30
            if counter >= 10:
                logger.error("Statement %s still present after retries", statement_name)
                return "failed to delete"
            time.sleep(timer)



def get_statement_api_json(config: dict, statement_name: str) -> dict:
    """GET /statements/{name}; matches legacy REST error payloads."""

    import httpx

    with flink_connection(config) as conn:
        resp = conn._request(
            f"/statements/{statement_name}",
            method="GET",
            raise_for_status=False,
        )  # noqa: SLF001
        if not isinstance(resp, httpx.Response):
            raise TypeError(f"unexpected response {type(resp)}")
        if resp.status_code == 404:
            try:
                return resp.json()
            except Exception:
                return {
                    "errors": [
                        {
                            "id": statement_name,
                            "status": "404",
                            "detail": "Statement not found",
                        }
                    ]
                }
        resp.raise_for_status()
        return resp.json()


def fetch_statement_results_terminal_json(config: dict, statement_name: str) -> dict | None:
    """

    Pagination over /results (same control flow as legacy statement_mgr)."""

    with flink_connection(config) as conn:
        next_page_token = None
        previous_step = None
        resp: dict | None = None
        while True:
            if next_page_token and previous_step != next_page_token:
                logger.info("Get next page token: %s for %s", next_page_token, statement_name)
                r = conn._request("GET", next_page_token, raise_for_status=False)  # noqa: SLF001
                status = getattr(r, "status_code", 200)
                if isinstance(status, int) and status >= 400:
                    break
                resp = r.json()
            else:
                logger.info("Get results from /statements/%s/results", statement_name)
                r = conn._request("GET", f"/statements/{statement_name}/results")  # noqa: SLF001
                resp = r.json()

            if not isinstance(resp, dict):
                break

            logger.info("response pagination same=%s", previous_step == next_page_token)
            md = resp.get("metadata") or {}
            nxt = md.get("next")
            if md and nxt:
                previous_step = next_page_token
                next_page_token = nxt
            else:
                try:
                    data = (resp.get("results") or {}).get("data")
                    logger.info("Data received for %s: data: %s", statement_name, data)
                except Exception:
                    pass
                break
        return resp


def get_statement_results_by_url_json(config: dict, absolute_url: str) -> dict | None:
    """Fetch one GET given a pagination URL (typically absolute)."""

    with flink_connection(config) as conn:
        resp = conn._request("GET", absolute_url)  # noqa: SLF001
        return resp.json()


def list_statements_first_page_json(config: dict, page_size: int) -> dict | None:
    with flink_connection(config) as conn:
        resp = conn._request(  # noqa: SLF001
            "/statements",
            params={"page_size": page_size},
        )
        return resp.json()


def list_statements_follow_page_json(config: dict, next_url_full: str) -> dict | None:
    """

    Paginate statements where metadata.next holds a fully-qualified HTTP URL."""

    with flink_connection(config) as conn:
        resp = conn._request("GET", next_url_full)  # noqa: SLF001
        return resp.json()


def patch_statement_stopped_using_rest(config: dict, statement_name: str, stopped: bool):
    """

    PATCH /spec/stopped using existing requests-based client."""

    cc = ConfluentCloudClient(config)
    url_base, auth = cc.build_flink_url_and_auth_header()
    data = [{"path": "/spec/stopped", "op": "replace", "value": stopped}]
    start_time = time.perf_counter()
    cc.make_request("PATCH", f"{url_base}/statements/{statement_name}", auth_header=auth, data=data)

    timer = float(config["flink"].get("poll_timer", 10))
    pending_counter = 0
    error_counter = 0
    compute_pool_id = config["flink"]["compute_pool_id"]

    while True:
        try:
            with flink_connection(config, compute_pool_id=compute_pool_id) as conn:
                drv = conn.get_statement(statement_name)
                phase = drv.phase.value
                if phase not in ("PENDING", "STOPPING"):
                    ps = driver_statement_to_pydantic(conn, drv)
                    ps.execution_time = time.perf_counter() - start_time  # type: ignore[attr-defined]
                    ps.loop_counter = pending_counter  # type: ignore[attr-defined]
                    logger.info(f"patch complete for {statement_name} phase={phase}")
                    return ps
        except OperationalError:
            raise
        except Exception as e:
            error_counter += 1
            if error_counter > 5:
                raise
            logger.warning("patch poll error for %s: %s", statement_name, e)
            time.sleep(timer)

        time.sleep(timer)
        pending_counter += 1
        if pending_counter % 3 == 0:
            timer += 10
            print(f"Wait {statement_name} patch, timer {timer}s")
        if pending_counter >= 23:
            raise TimeoutError(f"Timed out patching statement {statement_name}")
