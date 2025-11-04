import asyncio

from src import db


async def truncate_tables() -> None:
    await db.reset_fb_upload_data()
    await db.close_pool()


def main() -> None:
    asyncio.run(truncate_tables())


if __name__ == "__main__":
    main()
