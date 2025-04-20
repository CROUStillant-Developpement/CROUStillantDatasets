class BaseDataset:
    """
    Base class for all datasets.
    """
    def __init__(
        self, 
        name: str,
        dataset_id: str,
        dataset_description: str,
        dataset_slug: str,
        dataset_metadata: dict,
        resource_name: str,
        resource_description: str,
    ) -> None:
        """
        Initialise the Dataset class.

        :param name: Name of the dataset.
        :param dataset_id: ID of the dataset on data.gouv.fr.
        :param dataset_description: Description of the dataset.
        :param dataset_slug: Slug of the dataset on data.gouv.fr.
        :param dataset_metadata: Metadata of the dataset.
        :param resource_name: Name of the dataset resource.
        :param resource_description: Description of the dataset resource.
        """
        self.name = name
        self.dataset_id = dataset_id
        self.dataset_description = dataset_description
        self.dataset_slug = dataset_slug
        self.dataset_metadata = dataset_metadata
        self.dataset_metadata["description"] = self.dataset_description
        self.resource_name = resource_name
        self.resource_description = resource_description


    async def get_data(self) -> None:
        """
        Fetch the data from the PostgreSQL database
        """
        raise NotImplementedError("get_data() must be implemented in subclasses.")
