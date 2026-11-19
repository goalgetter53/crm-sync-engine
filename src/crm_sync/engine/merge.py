"""Field-level three-way merge.

Inputs per field: the warehouse value ``wh`` (after transform), the CRM value ``crm``, and the
base pair ``(base_wh, base_crm)`` = what each side held after the last successful sync. Keeping a
base *per side* (rather than one shared base) is what lets policies such as ``crm_wins`` leave the
two sides intentionally different without the next run mistaking that for a fresh change.

Resolution table (``dW`` = warehouse changed since base, ``dC`` = CRM changed since base)::

    record missing in CRM            -> warehouse value (create)
    direction to_warehouse           -> CRM value (written back; seeded from wh on create)
    never_overwrite                  -> CRM value (only creates set it)
    fill_if_empty                    -> CRM value if non-empty else warehouse value
    to_crm + source_wins             -> warehouse value, always (drift is reverted)
    wh == crm                        -> in sync
    dW only                          -> warehouse value
    dC only                          -> CRM value
    neither (sides differ)           -> to_crm: CRM value (status quo); bidirectional: policy
    both (sides differ) = CONFLICT   -> source_wins: wh | crm_wins: crm |
                                        latest_timestamp_wins: newer record timestamp, ties -> wh
"""

from __future__ import annotations

from dataclasses import dataclass

from crm_sync.dsl.models import FieldMapping
from crm_sync.values import CanonValue

BasePair = tuple[CanonValue, CanonValue]


@dataclass(frozen=True)
class FieldDecision:
    crm_property: str
    direction: str
    policy: str
    wh: CanonValue
    crm: CanonValue
    base: BasePair | None
    resolved: CanonValue
    write_crm: bool
    write_wh: bool
    conflict: bool
    reason: str

    def new_base(self, *, crm_written: CanonValue | None = None, wh_ok: bool = True) -> BasePair:
        """Base pair to persist once this decision has been applied."""
        b_wh = self.resolved if (self.write_wh and wh_ok) else self.wh
        if self.write_crm:
            b_crm = crm_written if crm_written is not None else self.resolved
        else:
            b_crm = self.crm
        return (b_wh, b_crm)


def _by_policy(
    policy: str, wh: CanonValue, crm: CanonValue, wh_ts: int | None, crm_ts: int | None
) -> tuple[CanonValue, str]:
    if policy == "crm_wins":
        return crm, "crm_wins"
    if policy == "latest_timestamp_wins":
        if crm_ts is not None and (wh_ts is None or crm_ts > wh_ts):
            return crm, "latest_timestamp_wins: CRM newer"
        return wh, "latest_timestamp_wins: warehouse newer or tie"
    return wh, "source_wins"


def merge_field(
    spec: FieldMapping,
    *,
    wh: CanonValue,
    crm: CanonValue,
    base: BasePair | None,
    crm_exists: bool,
    wh_ts: int | None = None,
    crm_ts: int | None = None,
) -> FieldDecision:
    d, p = spec.direction, spec.policy
    has_base = base is not None
    b_wh, b_crm = base if base is not None else (None, None)
    dw = (not has_base) or wh != b_wh
    dc = (not has_base) or crm != b_crm
    conflict = crm_exists and dw and dc and wh != crm

    resolved: CanonValue
    if not crm_exists:
        resolved, reason, conflict = wh, "create", False
    elif d == "to_warehouse":
        resolved, reason = crm, "CRM owns field"
    elif p == "never_overwrite":
        resolved, reason = crm, "never_overwrite: record already exists"
    elif p == "fill_if_empty":
        if crm is not None:
            resolved, reason = crm, "fill_if_empty: CRM value present"
        else:
            resolved, reason = wh, "fill_if_empty: CRM empty"
    elif d == "to_crm" and p == "source_wins":
        resolved = wh
        reason = "source_wins: revert CRM drift" if (dc and crm != wh) else "warehouse value"
    elif wh == crm:
        resolved, reason = wh, "in sync"
    elif dw and not dc:
        resolved, reason = wh, "warehouse changed"
    elif dc and not dw:
        resolved, reason = crm, "CRM changed"
    elif not dw and not dc:
        if d == "to_crm":
            resolved, reason = crm, "unchanged (CRM value retained by policy)"
        else:
            resolved, reason = _by_policy(p, wh, crm, wh_ts, crm_ts)
            reason = f"diverged: {reason}"
    else:
        resolved, reason = _by_policy(p, wh, crm, wh_ts, crm_ts)

    # to_warehouse fields are seeded from the warehouse on create, then owned by the CRM.
    write_crm = (
        (d != "to_warehouse" or not crm_exists)
        and resolved != crm
        and (crm_exists or resolved is not None)
    )
    write_wh = d != "to_crm" and crm_exists and resolved != wh
    return FieldDecision(
        crm_property=spec.crm,
        direction=d,
        policy=p,
        wh=wh,
        crm=crm,
        base=base,
        resolved=resolved,
        write_crm=write_crm,
        write_wh=write_wh,
        conflict=conflict,
        reason=reason,
    )
