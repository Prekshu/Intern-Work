"""

MODEL VERSIONS DATACLASS

Dataclass for collection of Model Versions

Copyright 2024, Ikigai Labs.
All rights reserved.

"""

from collections import namedtuple
from dataclasses import dataclass
from typing import ClassVar, Iterator, Optional, Type

from ikigai_component.components.model_version import ModelVersion
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.invalid_argument.invalid_input import (
    InvalidInputException,
)
from ikigai_proto.pypr_pb2 import PyprModelVersion

# To prevent database client from causing a circular dependency and satisfy F821
if False:
    from ikigai_network.database.client import DatabaseClient


@dataclass
class ModelVersions:
    """
    ModelVersions dataclass

    Class representing a collection of ModelVersion

    Fields:
        versions (list[ModelVersion]): List of model versions
    """

    # Attributes: can be accessed or updated directly
    versions: list[ModelVersion]

    DATABASE_TYPE: ClassVar[str] = DatabaseType.service
    DATABASE_TABLE: ClassVar[str] = ServiceTableType.model_version

    """
    Attribute configuration operations
    """

    def validate(self):
        """
        Check the validity of existing fields and add missing information if necessary
        Validity check should be performed BEFORE pushing to database
        """
        pass

    """
    Database operations
    """

    @classmethod
    def pull(
        cls,
        database_client: Type["DatabaseClient"],
        model_id: Optional[str] = None,
        model_ids: Optional[list[str]] = None,
    ):
        """
        Pull all model versions from the database for a given model

        Args:
            database_client (Type[DatabaseClient]): Database client
            model_id (str): Model ID
            model_ids (list[str]): List of Model IDs

        Returns:
            ModelVersions: ModelVersions object
        """

        if not model_id and not model_ids:
            raise InvalidInputException("Atleast one Model ID must be provided")

        versions: list[namedtuple] = cls._query(
            database_client=database_client,
            model_id=model_id,
            model_ids=model_ids,
        )

        return cls(
            [
                ModelVersion.from_namedtuple(obj_namedtuple=version)
                for version in versions
            ]
        )

    @classmethod
    def count(
        cls,
        database_client: Type["DatabaseClient"],
        model_id: str,
        model_ids: Optional[list[str]] = None,
    ) -> int:
        """
        Count the number of model versions for a given model

        Args:
            database_client (Type[DatabaseClient]): Database client
            model_id (str): Model ID
            model_ids (list[str]): List of Model IDs

        Returns:
            int: Number of model versions

        Raises:
            InvalidInputException: If model ID or project ID is not provided
        """

        if not model_id and not model_ids:
            raise InvalidInputException("Atleast one Model ID must be provided")

        versions: list[namedtuple] = cls._query(
            database_client=database_client,
            model_id=model_id,
            model_ids=model_ids,
        )

        return len(versions)

    @classmethod
    def delete(
        cls,
        database_client: Type["DatabaseClient"],
        model_id: str,
    ) -> None:
        """
        Delete all model versions for a given model

        Args:
            database_client (Type[DatabaseClient]): Database client
            model_id (str): Model ID
        """
        versions = cls.pull(
            database_client=database_client,
            model_id=model_id,
        )

        # Delete ModelVersions
        for version in versions.versions:
            version.delete(database_client=database_client)

    """
    Message Format operations
    """

    def to_proto(self, apply_formatting: bool) -> list[PyprModelVersion]:
        """
        Convert the ModelVersions object to a list of PyprModelVersion objects

        Returns:
            list[PyprModelVersion]: List of PyprModelVersion objects
        """
        versions = [
            version.to_proto(apply_formatting=apply_formatting)
            for version in self.versions
        ]
        return versions

    """
    Class utility methods
    """

    @classmethod
    def _query(
        cls,
        database_client: Type["DatabaseClient"],
        model_id: Optional[str] = None,
        model_ids: Optional[list[str]] = None,
    ) -> list[namedtuple]:
        """
        Get model version namedtuples from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            model_id (str): Model ID to filter ModelVersions by
            model_ids(list[str]): List of Model IDs
        Returns:
            list[namedtuple]: List of model version namedtuples
        """
        return database_client.query(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            model_id=model_id,
            model_ids=model_ids,
            fetchone=False,
        )
