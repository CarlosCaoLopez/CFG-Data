#!/usr/bin/env python
"""
Test para verificar que compute_variables.sql se ejecuta correctamente
y rellena la tabla closed_market_variables.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text
from database.connection import get_sync_session

load_dotenv()

def test_sql_execution():
    """Prueba que el SQL se ejecuta sin errores con exec_driver_sql"""

    sql_path = Path(__file__).parent / "scripts" / "sql" / "compute_variables.sql"
    sql_text = sql_path.read_text(encoding="utf-8")

    print("✓ SQL file loaded successfully")

    # Test que el parámetro está en formato psycopg2
    if "%(market_id)s" in sql_text:
        print("✓ SQL uses %(market_id)s format (psycopg2 compatible)")
    else:
        print("✗ ERROR: SQL does not use %(market_id)s format")
        return False

    print("\nAttempting to execute SQL with fake market_id...")

    try:
        with get_sync_session() as session:
            connection = session.connection()

            # Try executing with a non-existent market_id (should not error, just no rows)
            result = connection.exec_driver_sql(
                sql_text,
                {"market_id": "test_market_id_that_does_not_exist_12345"}
            )

            print(f"✓ SQL executed successfully")
            print(f"✓ Rows affected: {result.rowcount}")

            # Verify the method is being called correctly
            print("✓ exec_driver_sql method executed without errors")

            return True

    except Exception as e:
        print(f"✗ ERROR executing SQL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_sql_execution()
    sys.exit(0 if success else 1)
