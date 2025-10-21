from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


def is_http_event(event: Any) -> bool:
    return isinstance(event, dict) and "requestContext" in event


def _decode_body(event: Dict[str, Any]) -> Optional[str]:
    body = event.get("body")
    if not body:
        return None
    if event.get("isBase64Encoded"):
        return base64.b64decode(body).decode("utf-8")
    return body


def extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if not is_http_event(event):
        return event
    body = _decode_body(event)
    if not body:
        return {}
    return json.loads(body)


def http_response(body: Dict[str, Any], status_code: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


@dataclass(frozen=True)
class LambdaRequest:
    raw_event: Any
    payload: Dict[str, Any]
    is_http: bool

    @classmethod
    def parse(cls, event: Any) -> "LambdaRequest":
        payload = extract_payload(event)
        return cls(raw_event=event, payload=payload, is_http=is_http_event(event))

    def respond(self, body: Dict[str, Any], status_code: int = 200) -> Dict[str, Any]:
        if self.is_http:
            return http_response(body, status_code)
        return body

    def bad_request(self, message: str, status_code: int = 400) -> Dict[str, Any]:
        return self.respond({"status": "FAILED", "error": message}, status_code=status_code)

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

