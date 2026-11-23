from routewatch.config import load_postgres_settings
from routewatch.db import connect_postgres
from routewatch.raw import RAW_SCHEMA, initialize_raw_storage


def main() -> None:
    with connect_postgres(load_postgres_settings()) as connection:
        initialize_raw_storage(connection)

    print(f"Raw table ready: {RAW_SCHEMA}.messages")


if __name__ == "__main__":
    main()
