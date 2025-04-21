import os

from .utils.webhook import Webhook
from DataGouvPy import DataGouv
from asyncpg import Pool, create_pool
from pathlib import Path


class Worker:
    pool: Pool
    client: DataGouv
    notifications: Webhook

    def __init__(self, pool: Pool = None) -> None:
        """
        Initialize the Worker class.
        """
        self.pool = pool

        self.client = None
        self.notifications = None

        self.path = str(Path(__file__).parents[0].parents[0])


    @classmethod
    async def connect(cls, host: str, port: int, user: str, password: str, database: str) -> "Worker":
        """
        Connect to the PostgreSQL database.
        """
        pool = await create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

        return cls(pool=pool)


    async def setup(self, client, notifications) -> None:
        """
        Set the DataGouv client.
        """
        self.client = client
        self.notifications = notifications


    async def run(self):
        """
        Run the worker to load datasets.
        This function will dynamically load all datasets from the datasets directory and execute their get_data method.
        Then it will upload the dataset to the data.gouv.fr website via the API.
        """
        if not self.pool or not self.client:
            raise ValueError("Worker is not connected to the database or the data.gouv.fr client isn't initialised yet!")

        await self.notifications.send(
            "Lancement de la mise à jour des jeux de données du CROUStillant.\n\nCette tâche est exécutée automatiquement chaque jour à 01h00."
        )

        for dataset in os.listdir(self.path + "/CROUStillantDatasets/datasets"):
            if dataset.endswith(".py") and dataset != "__init__.py":
                dataset_name = dataset[:-3]

                module = __import__(f"CROUStillantDatasets.datasets.{dataset_name}", fromlist=["Dataset"])
                dataset_class = getattr(module, "Dataset")
                dataset_instance = dataset_class()

                data = await dataset_instance.get_data(self.pool)

                try:
                    dataset_data = await self.client.datasets.get_dataset(dataset_instance.dataset_id)

                    resource_id = dataset_data["resources"][0]["id"]

                    await self.client.datasets.update_dataset_resource(
                        dataset_id=dataset_instance.dataset_id,
                        resource_id=resource_id,
                        dataset=data,
                        resource_name=dataset_instance.resource_name,
                        resource_description=dataset_instance.resource_description,
                    )
                except Exception as e:
                    print(f"Error updating dataset {dataset_name}: {e}")

                    await self.notifications.send(
                        f"⚠️ Une erreur est survenue lors de la mise à jour du jeu de données `{dataset_instance.name}`"
                    )

                    continue

                print(f"Dataset {dataset_name} uploaded successfully.")

                await self.notifications.send(
                    f"Le jeu de données `{dataset_instance.name}` a été mis à jour avec succès !\n\nVous pouvez le retrouver ici : https://www.data.gouv.fr/fr/datasets/{dataset_instance.dataset_slug}/"
                )

        await self.notifications.send(
            "La mise à jour des jeux de données du CROUStillant est terminée !"
        )
