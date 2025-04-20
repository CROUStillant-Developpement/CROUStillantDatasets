import pandas as pd

from . import BaseDataset
from asyncpg import Pool, Connection
from datetime import datetime


class Dataset(BaseDataset):
    """
    Dataset class for the Restaurants dataset.
    """
    def __init__(self) -> None:
        """
        Initialise the restaurant Dataset class.
        """
        super().__init__(
            name="Restaurants",
            dataset_id="67d5b9d226ec985c447e87a4",
            dataset_description="L'ensemble des points de restauration du CROUS en France et en Outre-Mer.\n\nLes données sont préalablement nettoyées avant d'être téléversées.\n\nLes données sont mises à jour automatiquement chaque jour à 01h00.",
            dataset_slug="points-de-restauration-du-crous",
            dataset_metadata={
                "owner": "67b76d8d858371cef2625464",
                "title": "Points de restauration du CROUS", 
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
                    "ru"
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
            resource_name="points-de-restauration-crous.csv",
            resource_description="Points de restauration CROUS (Centre régional des œuvres universitaires et scolaires)",
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
                    RID,
                    R.IDREG AS IDREG,
                    R.LIBELLE AS REGION,
                    TPR.IDTPR AS IDTPR,
                    TPR.LIBELLE AS TYPE,
                    NOM,
                    ADRESSE,
                    LATITUDE,
                    LONGITUDE,
                    HORAIRES,
                    JOURS_OUVERT,
                    CASE 
                        WHEN IMAGE_URL IS NULL THEN NULL
                        ELSE CONCAT('https://api-croustillant.bayfield.dev/v1/restaurants/', RID, '/preview')
                    END AS IMAGE_URL,
                    EMAIL,
                    TELEPHONE,
                    ISPMR,
                    ZONE,
                    PAIEMENT,
                    ACCES,
                    OPENED,
                    AJOUT,
                    MIS_A_JOUR
                FROM restaurant
                JOIN region R ON restaurant.idreg = R.idreg
                JOIN type_restaurant TPR ON restaurant.idtpr = TPR.idtpr
                WHERE actif = TRUE
            """)
            
            df = pd.DataFrame.from_records(records)
            df.columns = [
                "restaurant_id",
                "region_id",
                "region",
                "type_id",
                "type",
                "nom",
                "adresse",
                "latitude",
                "longitude",
                "horaires",
                "jours_ouvert",
                "image_url",
                "email",
                "telephone",
                "acces_pmr",
                "zone",
                "paiement",
                "acces",
                "ouvert",
                "ajout",
                "mis_a_jour"
            ]

            return df
