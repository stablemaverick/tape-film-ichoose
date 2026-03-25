def build_idempotency_key(source: str, external_id: str) -> str:
    return f"{source}:{external_id}"
