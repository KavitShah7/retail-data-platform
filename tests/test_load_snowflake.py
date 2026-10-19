"""Verify transaction boundaries with fake connections; never contact Snowflake."""
from unittest.mock import Mock

import pytest

pytest.importorskip("snowflake.connector")
from python import load_snowflake as loader


@pytest.fixture
def database(monkeypatch):
    cursor = Mock()
    cursor.fetchall.return_value = [("file.parquet", "LOADED", 2, 2)]
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(loader, "connect_to_snowflake", lambda: connection)
    return connection, cursor


def statements(cursor):
    return [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]


def test_commit_only_after_both_loads(database):
    connection, cursor = database
    loader.main()
    sql = statements(cursor)
    begin = sql.index("BEGIN TRANSACTION")
    assert len([s for s in sql[:begin] if s.startswith("COPY INTO")]) == 2
    assert not any(s.startswith(("CREATE", "COPY")) for s in sql[begin:])
    assert sql[-1] == "COMMIT"
    assert "ROLLBACK" not in sql
    connection.close.assert_called_once()
    cursor.close.assert_called_once()


@pytest.mark.parametrize("results", [[], [("file", "LOADED", 0, 0)], [("file", "PARTIALLY_LOADED", 2, 1)]])
def test_bad_copy_never_deletes_destination(database, results):
    connection, cursor = database
    cursor.fetchall.side_effect = [[("file", "LOADED", 2, 2)], results]
    with pytest.raises(RuntimeError, match="Loading failed"):
        loader.main()
    assert not any(s.startswith(("DELETE", "BEGIN")) for s in statements(cursor))
    connection.close.assert_called_once()


def test_second_insert_failure_rolls_back_both_tables(database):
    connection, cursor = database

    def execute(sql):
        if " ".join(sql.split()).startswith("INSERT INTO CUSTOMER_SUMMARY"):
            raise RuntimeError("injected insert failure")

    cursor.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="injected insert failure"):
        loader.main()
    assert statements(cursor)[-1] == "ROLLBACK"
    assert "COMMIT" not in statements(cursor)
    connection.close.assert_called_once()


def test_failed_cursor_creation_closes_connection(database):
    connection, _ = database
    connection.cursor.side_effect = RuntimeError("cursor unavailable")
    with pytest.raises(RuntimeError, match="cursor unavailable"):
        loader.main()
    connection.close.assert_called_once()


def test_failed_cursor_close_still_closes_connection(database):
    connection, cursor = database
    cursor.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError, match="close failed"):
        loader.main()
    connection.close.assert_called_once()
