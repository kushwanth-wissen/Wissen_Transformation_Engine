from typing import Dict, Any, List
import os
import time
import requests
import json
import pandas as pd

from pyspark.sql import SparkSession, DataFrame


def _load_env(env_path: str) -> None:
    if not env_path:
        return
    if not os.path.exists(env_path):
        return
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def _extract_data(payload: Dict[str, Any], data_path: List[str] = None) -> List[Dict[str, Any]]:
    """
    Extract list of records from API response payload.
    Attempts common locations: payload['data'], payload['results'], else if payload is list return it.
    data_path optionally can be list of keys to follow.
    """
    if data_path:
        cur = payload
        for key in data_path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                return []
        if isinstance(cur, list):
            return cur
        return []

    # common keys
    for key in ("data", "results", "items"):
        if isinstance(payload, dict) and key in payload and isinstance(payload[key], list):
            return payload[key]

    # if payload itself is a list
    if isinstance(payload, list):
        return payload

    # fallback: try to find first list value in dict
    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                return v

    return []


def read_api(spark: SparkSession, config: Dict[str, Any], logger) -> DataFrame:
    """
    Reads data from a paginated REST API according to configuration.

    Config expectations (example in your prompt):
    - url: base url
    - method: GET/POST
    - params: dict of query params
    - headers: dict of headers (optional)
    - auth: {
        type: 'api_key', in: 'params'|'headers', param_name: 'access_key', env_key: 'ENV_VAR'
      }
    - pagination: { enabled: bool, type: 'page'|'offset'|'cursor', page_param: 'page', start: 0, increment: 1, page_size_param: 'limit', page_size: 100, cursor_param: 'cursor' }
    - rate_limit: { enabled: bool, delay_seconds: float }
    - data_path: optional list of keys to reach array inside JSON
    """
    url = config.get("url")
    method = config.get("method", "GET").upper()
    params = dict(config.get("params", {}))
    headers = dict(config.get("headers", {}))
    auth = config.get("auth", {}) or {}
    pagination = config.get("pagination", {}) or {}
    rate_limit = config.get("rate_limit", {}) or {}
    data_path = config.get("data_path")  # optional list or dot-separated string

    if not url:
        raise ValueError("API source requires a 'url' in config")

    # Load auth from env if specified
    if auth and auth.get("type") == "api_key":
        env_key = auth.get("env_key")
        key_value = None
        if env_key:
            key_value = os.environ.get(env_key)
        # if provided directly in config param, use it
        if not key_value and auth.get("value"):
            key_value = auth.get("value")
        if not key_value:
            raise ValueError("API auth requires an env_key or value for api_key authentication")
        location = auth.get("in", "params")
        param_name = auth.get("param_name", "access_key")
        if location == "params":
            params[param_name] = key_value
        else:
            headers[param_name] = key_value

    # If credentials env is provided at top-level (legacy), load it
    credentials_env = config.get("credentials_env")
    if credentials_env:
        _load_env(credentials_env)

    # normalize data_path
    if isinstance(data_path, str):
        data_path = [p for p in data_path.split('.') if p]

    page_enabled = pagination.get("enabled", False)
    results: List[Dict[str, Any]] = []

    # rate limit
    rl_enabled = rate_limit.get("enabled", False)
    rl_delay = float(rate_limit.get("delay_seconds", 1.0))

    # Pagination handling
    if page_enabled:
        ptype = pagination.get("type", "page")
        # page variables
        start = int(pagination.get("start", 0))
        increment = int(pagination.get("increment", 1))
        page_param = pagination.get("page_param", "page")
        page_size_param = pagination.get("page_size_param")
        page_size = pagination.get("page_size")
        cursor_param = pagination.get("cursor_param", "cursor")

        # fetch loop
        current = start
        more = True
        max_cycles = int(pagination.get("max_pages", 1000))
        cycles = 0
        while more and cycles < max_cycles:
            cycles += 1
            # build request params for this page
            req_params = dict(params)
            if ptype in ("page", "offset"):
                req_params[page_param] = current
                if page_size_param and page_size is not None:
                    req_params[page_size_param] = page_size
            elif ptype == "cursor":
                # for cursor-based, current holds cursor token
                if current:
                    req_params[cursor_param] = current

            # make request
            try:
                resp = requests.request(method, url, params=req_params, headers=headers, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"API request failed: {e}")
                raise

            try:
                payload = resp.json()
            except Exception:
                # try parse as text
                try:
                    payload = json.loads(resp.text)
                except Exception as e:
                    logger.error(f"Could not parse API response as JSON: {e}")
                    raise

            batch = _extract_data(payload, data_path)
            results.extend(batch)

            # determine continuation
            if ptype == "page":
                # if returned less than page_size, stop
                if page_size and len(batch) < page_size:
                    more = False
                else:
                    current += increment
            elif ptype == "offset":
                if page_size and len(batch) < page_size:
                    more = False
                else:
                    current += increment
            elif ptype == "cursor":
                # look for next cursor in payload (common keys: next, cursor, pagination.cursor)
                next_cursor = None
                for key in ("next", "cursor", "next_cursor", "pagination"):
                    val = payload.get(key)
                    if isinstance(val, str) and val:
                        next_cursor = val
                        break
                    if isinstance(val, dict) and val.get("cursor"):
                        next_cursor = val.get("cursor")
                        break
                if next_cursor:
                    current = next_cursor
                else:
                    more = False

            # apply rate limiting
            if rl_enabled and rl_delay > 0:
                time.sleep(rl_delay)

    else:
        # single request
        try:
            resp = requests.request(method, url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"API request failed: {e}")
            raise

        try:
            payload = resp.json()
        except Exception:
            try:
                payload = json.loads(resp.text)
            except Exception as e:
                logger.error(f"Could not parse API response as JSON: {e}")
                raise

        results = _extract_data(payload, data_path)

    # Convert results (list of dict) into Spark DataFrame
    try:
        if not results:
            # return empty df
            return spark.createDataFrame([], schema=None)
        # use pandas to normalize nested JSON
        pdf = pd.json_normalize(results)
        df = spark.createDataFrame(pdf)
        return df
    except Exception as e:
        logger.error(f"Error converting API results to DataFrame: {e}")
        raise
