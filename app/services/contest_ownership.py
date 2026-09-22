"""One-time, explicit ownership migration for legacy contest staff."""
import json
import os

from sqlalchemy import text


def backfill_contest_owners(connection, assignments: dict[str, str] | None = None) -> None:
    # Ambiguous legacy contests must be assigned deliberately, never by email sort.
    assignments = assignments if assignments is not None else json.loads(os.environ.get("CONTEST_OWNER_MIGRATION_ASSIGNMENTS", "{}"))
    staff = [dict(row) for row in connection.execute(text(
        "SELECT staff_account_id, email, contest_scopes, contest_roles, protected_master_contests "
        "FROM staff_accounts WHERE is_service_master = false"
    )).mappings()]
    for account in staff:
        for key, empty in [("contest_scopes", "{}"), ("contest_roles", "{}"), ("protected_master_contests", "[]")]:
            account[key] = json.loads(account[key] or empty)
    contests = connection.execute(text("SELECT contest_id FROM contests WHERE owner_staff_account_id IS NULL")).scalars().all()
    for cid in contests:
        members = [a for a in staff if a["contest_scopes"].get(cid)]
        if not members:
            continue
        masters = [a for a in members if "contest.*" in a["contest_scopes"][cid]]
        protected = [a for a in masters if cid in a["protected_master_contests"]]
        specified = assignments.get(cid, assignments.get("*", "") if len(protected or masters) != 1 else "").strip().lower()
        candidates = [a for a in masters if a["email"].lower() == specified] if specified else (protected or masters)
        if len(candidates) != 1:
            raise RuntimeError(f"Contest {cid} needs an explicit initial owner in CONTEST_OWNER_MIGRATION_ASSIGNMENTS.")
        owner = candidates[0]
        for account in members:
            account["protected_master_contests"] = [value for value in account["protected_master_contests"] if value != cid]
            if account is owner:
                account["contest_roles"][cid] = ["owner"]
                account["contest_scopes"][cid] = ["contest.*", "contest.owner"]
                account["protected_master_contests"].append(cid)
            connection.execute(text(
                "UPDATE staff_accounts SET contest_scopes=:scopes, contest_roles=:roles, protected_master_contests=:protected WHERE staff_account_id=:id"
            ), {"scopes": json.dumps(account["contest_scopes"]), "roles": json.dumps(account["contest_roles"]), "protected": json.dumps(account["protected_master_contests"]), "id": account["staff_account_id"]})
        connection.execute(text("UPDATE contests SET owner_staff_account_id=:owner WHERE contest_id=:cid"), {"owner": owner["staff_account_id"], "cid": cid})
