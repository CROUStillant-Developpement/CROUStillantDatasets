import pandas as pd

from . import BaseDataset
from asyncpg import Pool, Connection
from datetime import datetime


class Dataset(BaseDataset):
    """
    Dataset class for the Regions dataset.
    """
    def __init__(self) -> None:
        """
        Initialise the regions Dataset class.
        """
        super().__init__(
            name="Regions",
            dataset_id="68053f2dee3c00002e6ca4e5",
            dataset_description="L'ensemble des régions du CROUS en France et en Outre-Mer.\n\nLes données sont préalablement nettoyées avant d'être téléversées.\n\nLes données sont mises à jour automatiquement chaque jour à 01h00.\n\nVous pouvez aussi retrouver toutes les données via notre API : https://www.data.gouv.fr/fr/dataservices/api-croustillant/",
            dataset_slug="regions-du-crous",
            dataset_metadata={
                "owner": "67b76d8d858371cef2625464",
                "title": "Régions du CROUS", 
                "private": False,
                "description": None, # Description is set in the constructor
                "acronym": "CROUStillant", 
                "tags": [
                    "crous",
                    "les-crous",
                    "restaurant",
                    "restaurants",
                    "restaurants-adminstratif",
                    "restaurants-universitaire",
                    "ru",
                    "regions",
                    "region",
                ],
                "license": "lov2", 
                "frequency": "daily",
                "temporal_coverage": {
                    "start": "2025-01-01T00:00:00.000Z",
                    "end": datetime.now().isoformat()
                },
                "spatial": {
                    "granularity":"poi"
                }
            },
            resource_name="regions.csv",
            resource_description="Liste des régions du CROUS (Centre régional des œuvres universitaires et scolaires)",
        )


    async def get_data(self, pool: Pool) -> pd.DataFrame:
        """
        Fetch the data from the PostgreSQL database.

        :param pool: Connection pool to the PostgreSQL database.
        :return: DataFrame containing the data.
        """
        async with pool.acquire() as connection:
            connection: Connection

            records = await connection.fetch("""
                SELECT 
                    IDREG,
                    LIBELLE
                FROM region
            """)
            
            df = pd.DataFrame.from_records(records)
            df.columns = [
                "region_id",
                "region"
            ]

            return df
