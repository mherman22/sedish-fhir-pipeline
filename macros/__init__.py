from __future__ import annotations

from sqlglot import exp
from sqlmesh import macro


@macro()
def fhir_id(evaluator, expr: exp.Expression) -> exp.Expression:
    """Coerce any source value into a valid FHIR resource id / reference target.

    FHIR R4 ids must match ``[A-Za-z0-9.-]{1,64}`` and HAPI additionally rejects
    all-numeric client-assigned ids (HAPI-0960). Source data is not guaranteed to
    comply: e.g. seeded test rows carry uuids like
    ``test-73106-encounter_openmrs-1014`` (illegal ``_`` -> HAPI-0521), and the
    ``locations.value_reference`` code is purely numeric (-> HAPI-0960).

    This replaces every illegal character with ``-`` and prefixes ``id-`` when the
    result would otherwise be all digits. It is deterministic and idempotent, so a
    resource id and every reference that points to it transform identically and stay
    consistent. A clean uuid passes through unchanged.
    """
    col = expr.sql(dialect=evaluator.dialect)
    sanitized = f"REGEXP_REPLACE(CAST({col} AS CHAR), '[^A-Za-z0-9.-]', '-')"
    sql = (
        f"CASE WHEN {sanitized} REGEXP '^[0-9]+$' "
        f"THEN CONCAT('id-', {sanitized}) ELSE {sanitized} END"
    )
    return exp.maybe_parse(sql, dialect=evaluator.dialect)
