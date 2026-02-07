from aiohttp import ClientSession
from discord import Webhook as DiscordWebhook
from datetime import datetime
from pytz import timezone

from .views import WorkerView
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path=".env")


class Webhook:
    """
    Webhook class to handle webhooks notifications.
    """
    def __init__(self, session: ClientSession, url: str) -> None:
        """
        Initialise the Webhook class.

        :param session: ClientSession to use for the webhook.
        :param url: URL of the webhook.
        """
        self.webhook = DiscordWebhook.from_url(url, session=session)


    async def send(self, description: str) -> None:
        """
        Send a message to the webhook.

        :param description: Description of the message.
        """
        year = datetime.now(timezone("Europe/Paris")).year

        view = WorkerView(
            content=description,
            thumbnail_url=environ["THUMBNAIL_URL"],
            banner_url=environ["IMAGE_URL"],
            footer_text="CROUStillant Développement © 2022 - {year} | Tous droits réservés.".format(year=year),
        )

        await self.webhook.send(view=view)
