"""

MODELS DATACLASS

Dataclass for collection of models

Copyright 2024, Ikigai Labs.
All rights reserved.

"""
from collections import namedtuple
from dataclasses import dataclass
from typing import ClassVar, Iterator, Optional, Type

from ikigai_component.alias.alias_utils import is_alias, to_project_component_id
from ikigai_component.components.model import Model
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.invalid_argument.invalid_input import (
    InvalidInputException,
)
from ikigai_proto.pypr_pb2 import PyprModel

# To prevent database client from causing a circular dependency and satisfy F821
if False:
    from ikigai_network.database.client import DatabaseClient


@dataclass
class Models:
    """
    Models dataclass

    Class representing a collection of Models

    Fields:
        models (list[Model]): List of Models
    """

    # Attributes: can be accessed or updated directly
    models: list[Model]

    DATABASE_TYPE: ClassVar[str] = DatabaseType.service
    DATABASE_TABLE: ClassVar[str] = ServiceTableType.model

    """
    Models operations
    """

    def __len__(self) -> int:
        """
        Get the number of models in the collection

        Returns:
            int: Number of models
        """
        return len(self.models)

    def __iter__(self) -> Iterator[Model]:
        """
        Iterate over the models in the collection

        Returns:
            Iterator[Model]: Iterator over Model
        """
        return iter(self.models)

    """
    Attribute configuration operations
    """

    def validate(self):
        """
        Check the validity of existing fields and add missing information if necessary
        Validity check should be performed BEFORE pushing to database
        """
        pass

    @classmethod
    def resolve_directory_alias(
        cls, directory_id, project_id, database_client: Type["DatabaseClient"]
    ):
        """
        Convert alias back to the component id, if necessary

        Args:
            directory_id (str):
            project_id (str):
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InvalidInputException: "Project must be provided with an alias"
        """
        if not is_alias(directory_id):
            return directory_id
        if not project_id:
            raise InvalidInputException("Project must be provided with an alias")

        return to_project_component_id(
            database_client=database_client,
            identifier=directory_id,
            project_id=project_id,
        )

    """
    Database operations
    """

    @classmethod
    def pull(
        cls,
        database_client: Type["DatabaseClient"],
        project_id: str,
        directory_id: Optional[str] = None,
    ):
        """
        Pull all models from the database that match the provided filters

        Args:
            database_client (Type[DatabaseClient]): Database client
            project_id (str): Project ID
            directory_id (str): Directory ID to optionally filter by

        Returns:
            Models: Models object

        Raises:
            InvalidInputException: If project_id is not provided
        """
        if not project_id:
            raise InvalidInputException("Project ID must be provided")

        directory_id = directory_id if directory_id else None
        # Resolve alias if needed
        if directory_id:
            directory_id = cls.resolve_directory_alias(
                directory_id=directory_id,
                project_id=project_id,
                database_client=database_client,
            )

        models: list[namedtuple] = [
            Model.from_namedtuple(obj_namedtuple=model)
            for model in cls._query(
                database_client=database_client,
                project_id=project_id,
                directory_id=directory_id,
            )
        ]

        return cls(models=models)

    @classmethod
    def count(
        cls,
        database_client: Type["DatabaseClient"],
        project_id: str,
        directory_id: Optional[str] = None,
    ) -> int:
        """
        Count the number of models in a project

        Args:
            database_client (Type[DatabaseClient]): Database client
            project_id (str): Project ID
            directory_id (str): Directory ID to optionally filter by

        Returns:
            int: Number of models in the project

        Raises:
            InvalidInputException: If project ID is not provided
        """
        if not project_id:
            raise InvalidInputException("Project ID must be provided")

        directory_id = directory_id if directory_id else None
        # Resolve alias if needed
        if directory_id:
            directory_id = cls.resolve_directory_alias(
                directory_id=directory_id,
                project_id=project_id,
                database_client=database_client,
            )

        models = cls._query(
            database_client=database_client,
            project_id=project_id,
            directory_id=directory_id,
        )

        return len(models)

    @classmethod
    def delete(
        cls,
        database_client: Type["DatabaseClient"],
        project_id: str,
        directory_id: Optional[str] = None,
    ) -> None:
        """
        Delete all models in a project

        Args:
            database_client (Type[DatabaseClient]): Database client
            project_id (str): Project ID
            directory_id (str): Directory ID to optionally filter by
        """
        models = cls.pull(
            database_client=database_client,
            project_id=project_id,
            directory_id=directory_id,
        )

        # Delete Models
        for model in models.models:
            model.delete(database_client=database_client)

    """
    Message Format operations
    """

    @classmethod
    def from_proto(
        cls,
        obj_protos: list[PyprModel],
        database_client: Type["DatabaseClient"],
    ):
        """
        Convert a list of PyprModel objects to a Models object

        Args:
            obj_protos (List[PyprModel]): List of PyprModel proto objects
            database_client (Type[DatabaseClient]): Database client

        Returns:
            Models: Models object containing a list of Model objects
        """
        models = []

        for obj_proto in obj_protos:
            model = Model.from_proto(
                obj_proto=obj_proto, database_client=database_client
            )
            models.append(model)

        return cls(models=models)

    def to_proto(self) -> list[PyprModel]:
        """
        Convert the Models object to a list of PyprModel objects

        Returns:
            list[PyprModel]: List of PyprModel objects
        """
        models = [model.to_proto() for model in self.models]
        return models

    """
    Class utility methods
    """

    @classmethod
    def _query(
        cls,
        database_client: Type["DatabaseClient"],
        project_id: Optional[str] = None,
        directory_id: Optional[str] = None,
    ) -> list[namedtuple]:
        """
        Get model namedtuples from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            project_id (str): Project ID to filter Models by
            directory_id (str): Directory ID to filter Dashboards by
        Returns:
            list[namedtuple]: List of model namedtuples
        """
        return database_client.query(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            project_id=project_id,
            directory_id=directory_id,
            fetchone=False,
        )
