from __future__ import annotations

import datetime as _dt
import logging
import os
from decimal import Decimal
from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_CE_CLIENT = boto3.client("ce", region_name=os.environ.get("AWS_REGION", "us-east-1"))

COST_LIMIT_ENV = "COST_LIMIT_USD"


def _month_range(today: _dt.date) -> Dict[str, str]:
    start = today.replace(day=1)
    # Cost Explorer end date is exclusive; add a day to include today.
    end = today + _dt.timedelta(days=1)
    return {"Start": start.isoformat(), "End": end.isoformat()}


def _current_month_cost() -> Dict[str, Any]:
    period = _month_range(_dt.date.today())
    response = _CE_CLIENT.get_cost_and_usage(
        TimePeriod=period,
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
    )
    results = response.get("ResultsByTime", [])
    total = results[0]["Total"]["UnblendedCost"] if results else {"Amount": "0", "Unit": "USD"}
    amount = float(Decimal(total.get("Amount", "0")))
    currency = total.get("Unit", "USD")
    return {"amount": amount, "currency": currency, "time_period": period}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    limit_raw = os.environ.get(COST_LIMIT_ENV)
    if not limit_raw:
        raise RuntimeError(f"{COST_LIMIT_ENV} environment variable must be set.")

    try:
        limit = float(limit_raw)
    except ValueError as exc:  # noqa: PERF203
        raise RuntimeError(f"Invalid {COST_LIMIT_ENV}: {limit_raw}") from exc

    try:
        cost = _current_month_cost()
    except (ClientError, BotoCoreError) as exc:
        logger.error("Failed to query Cost Explorer: %s", exc)
        return {
            "status": "ERROR",
            "allowed": False,
            "error": "cost_explorer_unavailable",
            "limit": limit,
        }

    amount = cost["amount"]
    allowed = amount < limit
    return {
        "status": "OK",
        "allowed": allowed,
        "current_spend": amount,
        "limit": limit,
        "currency": cost.get("currency", "USD"),
        "time_period": cost.get("time_period"),
    }
