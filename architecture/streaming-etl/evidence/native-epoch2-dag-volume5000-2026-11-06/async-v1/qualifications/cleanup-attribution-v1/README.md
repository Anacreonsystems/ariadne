# Operational cleanup attribution qualification

The original driver and Principal receipts remain exact. Their shorthand about dropping two fresh databases must be read with this qualification. What was observed is two fresh scoped names absent after driver cleanup and zero sessions at the driver check. The driver recorded `dropped` after `DROP IF EXISTS` without recording prior existence, so it did not establish that it personally removed two existing databases.

The unchanged [d906 conftest source](../../source/tests/conftest.py.log), `generate_test_database_connection_url` lines425–499, attempts `pg_terminate_backend` plus DROP/CREATE for the derived fresh `_tests_master` during setup and DROP IF EXISTS at teardown. Earlier framework cleanup/session preconditions were not instrumented. Zero sessions at the later driver check and its own `force_used=false` flag do not describe every earlier fixture cleanup action. Unique fresh-name guards exclude old soak/forensic targets.

This limits operational attribution. It does not invalidate the functional5000 result, exact effects, whole-health observations or recorded11 native PID closures. No run was repeated and no original receipt/index was changed. See [machine-readable qualification](qualification.json).
