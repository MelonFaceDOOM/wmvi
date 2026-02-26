from __future__ import annotations

def normalize_service_id(s: str) -> str:
    s = s.strip().replace("\\", "/").strip("/")
    if not s:
        raise ValueError("Empty service id")
    if ".." in s.split("/"):
        raise ValueError("Invalid service id (.. not allowed)")
    return s

def unit_name_from_service_id(service_id: str) -> str:
    return service_id.replace("/", "_").replace("-", "_")

def module_from_service_id(service_id: str) -> str:
    return "services." + service_id.replace("/", ".").replace("-", "_")