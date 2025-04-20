import asyncio

from CROUStillantDatasets.worker import Worker
from CROUStillantDatasets.utils.webhook import Webhook
from DataGouvPy import DataGouv
from dotenv import load_dotenv
from os import environ
from aiohttp import ClientSession


load_dotenv(dotenv_path=".env")


async def main():
    """
    Main function to run the script.
    """
    session = ClientSession()

    notifications = Webhook(
        session=session,
        url=environ.get("DISCORD_WEBHOOK_URL")
    )

    client = DataGouv(
        session=session,
        api_key=environ.get("DATA_GOUV_API_KEY"),
    )

    worker = await Worker.connect(
        host=environ.get("POSTGRES_HOST"),
        port=int(environ.get("POSTGRES_PORT")),
        user=environ.get("POSTGRES_USER"),
        password=environ.get("POSTGRES_PASSWORD"),
        database=environ.get("POSTGRES_DATABASE")
    )
    await worker.setup(client, notifications)
    await worker.run()

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
