if __name__ == "__main__":
    import dedupe
    from api.database import init_engine
    from api.app_config import DB_CONN
    from api.models import DedupeSession

    engine = init_engine(DB_CONN)

    from api.database import app_session
    from api.utils.delayed_tasks import trainDedupe

    dedupe_sessions = app_session.query(DedupeSession).all()

    for session in dedupe_sessions:
        trainDedupe(session.id)
