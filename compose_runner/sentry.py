import os
import sentry_sdk

if not (
    os.environ.get("PYTEST_CURRENT_TEST")
    or os.environ.get("CI")
    or os.environ.get("DISABLE_SENTRY")
):
    sentry_sdk.init(
        dsn="https://9385c05482031864cf4cff4761d714f0@o4505036784992256.ingest.us.sentry.io/4509758855970816",
        send_default_pii=True,
    )
