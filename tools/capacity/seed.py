"""Create synthetic workload in an explicitly isolated capacity-test database."""
import json
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import insert, select, func
from sqlalchemy.engine import make_url

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import (
    ContestRow, ContestDivisionRow, ParticipantTeamRow, TeamMemberRow,
    TeamSessionRow, ProblemRow, SubmissionRow, JudgeJobRow,
)
from app.services.security import new_session_token, token_hash
from app.settings import settings


def main():
    assert (make_url(settings.database_url).database or "").startswith("zoj_capacity_"), "Refuse to seed a service database"
    assert settings.app_env == "capacity", "This command is for a disposable capacity environment"
    from app.services.store import store  # creates schema only in the test database
    now = now_utc()
    actors = []
    with SessionLocal() as db:
        assert not db.scalar(select(func.count()).select_from(ContestRow)), "Database is not empty"
        for ci in range(3):
            cid, did = str(uuid4()), str(uuid4())
            db.add(ContestRow(contest_id=cid, title=f"CAPACITY ONLY {ci}",
                organization_name="Synthetic benchmark", overview="Temporary synthetic workload",
                status="running", start_at=now-timedelta(hours=1),
                end_at=now+timedelta(days=1), freeze_at=now+timedelta(hours=20)))
            db.flush()
            db.add(ContestDivisionRow(division_id=did, contest_id=cid, code="A", name="Capacity"))
            db.flush()
            pids = [str(uuid4()) for _ in range(10)]
            db.execute(insert(ProblemRow), [dict(problem_id=pid, contest_id=cid, division_id=did,
                problem_code=chr(65+i), title=f"Synthetic {i}", statement="Compute a+b.",
                time_limit_ms=1000, memory_limit_mb=128, language_resource_limits={}, display_order=i+1)
                for i, pid in enumerate(pids)])
            for ai in range(1000):
                tid, mid = str(uuid4()), str(uuid4())
                email = f"capacity-{ci}-{ai}@example.com"
                db.add(ParticipantTeamRow(participant_team_id=tid, contest_id=cid, division_id=did,
                    team_name=f"Synthetic {ai:04d}", status="active"))
                db.flush()
                db.add(TeamMemberRow(team_member_id=mid, contest_id=cid, participant_team_id=tid,
                    role="leader", name="Synthetic participant", email=email))
                db.flush()
                token = new_session_token("participant_access", mid, 86400,
                    {"contest_id":cid, "team_id":tid, "division_id":did})
                db.add(TeamSessionRow(contest_id=cid, division_id=did, participant_team_id=tid,
                    team_member_id=mid, access_token_hash=token_hash(token),
                    expires_at=now+timedelta(days=1), last_seen_at=now))
                rows = [dict(submission_id=str(uuid4()), contest_id=cid, division_id=did,
                    participant_team_id=tid, team_member_id=mid, problem_id=pids[si%10],
                    language="cpp17", source_code="// synthetic history\n" + " "*2048,
                    status=("judging" if si == 19 else ("accepted" if si%3==0 else "wrong_answer")),
                    submitted_at=now-timedelta(minutes=40-si)) for si in range(20)]
                db.execute(insert(SubmissionRow), rows)
                actors.append(dict(contest_id=cid, division_id=did, problem_id=pids[0],
                    token=token, submission_id=rows[-1]["submission_id"]))
            db.commit()
    path = Path("/capacity/actors.json")
    path.write_text(json.dumps(actors))
    path.chmod(0o600)
    print(json.dumps({"contests":3,"participants":len(actors),"history_submissions":len(actors)*20}))


if __name__ == "__main__":
    main()
