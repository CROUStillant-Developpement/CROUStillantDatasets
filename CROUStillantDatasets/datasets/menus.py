import pandas as pd

from . import BaseDataset
from asyncpg import Pool, Connection
from datetime import datetime


class Dataset(BaseDataset):
    """
    Dataset class for the Menus dataset.
    """
    def __init__(self) -> None:
        """
        Initialise the menus Dataset class.
        """
        super().__init__(
            name="Menus",
            dataset_id="69877dbf19bd669b8d1d7579",
            dataset_description="L'ensemble des menus proposés dans les restaurants universitaires du CROUS en France et en Outre-Mer.\n\nLes données sont préalablement nettoyées avant d'être téléversées.\n\nLes données sont mises à jour automatiquement chaque jour à 01h00.\n\nVous pouvez aussi retrouver toutes les données via notre API : https://www.data.gouv.fr/fr/dataservices/api-croustillant/",
            dataset_slug="menus-du-crous",
            dataset_metadata={
                "owner": "67b76d8d858371cef2625464",
                "title": "Menus du CROUS", 
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
                    "menus",
                    "menu",
                    "repas",
                    "plats",
                    "plat",
                    "categorie",
                    "categories",
                    "plateau-repas",
                    "plateaux-repas",
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
            resource_name=f"menus-du-crous-{datetime.now().strftime('%d-%m-%Y')}.csv",
            resource_description="Menus du CROUS (Centre régional des œuvres universitaires et scolaires)",
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
                    M.MID AS menu_id,
                    M.DATE AS menu_date,
                    RP.RPID AS repas_id,
                    RP.TPR AS repas_type,
                    C.CATID AS categorie_id,
                    C.TPCAT AS categorie_type,
                    C.ORDRE AS categorie_ordre,
                    P.PLATID AS plat_id,
                    P.LIBELLE AS plat_libelle,
                    CO.ORDRE AS plat_ordre,
                    R.RID AS restaurant_id,
                    M.MENU_HASH AS menu_hash,
                    M.ingestion_at AS ingestion_timestamp,
                    M.last_checked AS last_checked_timestamp
                FROM PUBLIC.MENU M
                JOIN PUBLIC.RESTAURANT R ON M.RID = R.RID
                JOIN PUBLIC.REPAS RP ON M.MID = RP.MID
                JOIN PUBLIC.CATEGORIE C ON RP.RPID = C.RPID
                JOIN PUBLIC.COMPOSITION CO ON C.CATID = CO.CATID
                JOIN PUBLIC.PLAT P ON CO.PLATID = P.PLATID
                WHERE M.DATE = CURRENT_DATE - INTERVAL '1 days'
                ORDER BY M.DATE, RP.RPID, C.ORDRE, CO.ORDRE
            """)

            df = pd.DataFrame.from_records(records)
            df.columns = [
                "menu_id",
                "menu_date",
                "repas_id",
                "repas_type",
                "categorie_id",
                "categorie_type",
                "categorie_ordre",
                "plat_id",
                "plat_libelle",
                "plat_ordre",
                "restaurant_id",
                "menu_hash",
                "ingestion_timestamp",
                "last_checked_timestamp"   
            ]

            return df
