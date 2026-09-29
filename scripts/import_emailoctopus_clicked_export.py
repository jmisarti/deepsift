"""Load an EmailOctopus contact export as conservative historical click evidence."""

import argparse
import json

import app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path")
    args = parser.parse_args()

    # This also runs the one-time retained-webhook migration first, preventing
    # an export row from duplicating a click already known through a callback.
    app.ensure_db()
    db = app.open_sqlite_connection()
    try:
        result = app.import_emailoctopus_clicked_export(db, args.csv_path)
        app.commit_with_retry(db)
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
