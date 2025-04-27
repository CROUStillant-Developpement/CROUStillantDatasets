from aiohttp import ClientSession
from discord import Embed, Webhook as DiscordWebhook
from datetime import datetime
from pytz import timezone


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
        embed = Embed(
            title="CROUStillant Datasets",
            description=description,
            color=int("6A9F56", base=16),
            timestamp=datetime.now()
        )
        embed.set_footer(text=f"CROUStillant Développement © 2022 - {datetime.now(timezone('Europe/Paris')).year} | Tous droits réservés")
        embed.set_image(url="https://croustillant.menu/banner-small.png")

        await self.webhook.send(embed=embed)
