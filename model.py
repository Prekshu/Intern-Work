"""

MODEL DATACLASS

Dataclass for a Model

Copyright 2024, Ikigai Labs.
All rights reserved.

"""
import os
import re
from collections import namedtuple
from dataclasses import dataclass
from typing import ClassVar, Optional, Type, Union

from ikigai_cloud.object_store import ObjectStore
from ikigai_cloud.utils.message_utils import OBJECT_STORE_BUCKET
from ikigai_component.alias.alias_utils import (
    delete_alias_for_component,
    is_alias,
    to_project_component_id,
)
from ikigai_component.components.model_versions import ModelVersions
from ikigai_model.utils.format_utils import (
    format_external_model_type_to_model_type,
    format_model_type_to_external_model_type,
)
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.internal.database import DatabaseException
from ikigai_network.exceptions.invalid_argument.invalid_input import (
    InvalidInputException,
)
from ikigai_network.exceptions.not_found.no_directory import NoDirectoryException
from ikigai_network.exceptions.not_found.no_model import NoModelException
from ikigai_network.utils.message_utils import COMPONENT_TYPES
from ikigai_proto.pypr_pb2 import PyprDirectory, PyprModel
from pathvalidate import ValidationError, validate_filename

# To prevent database client from causing a circular dependency and satisfy F821
if False:
    from ikigai_network.database.client import DatabaseClient


@dataclass
class Model:
    """
    Model dataclass

    Class containing information about a Model

    Fields:
        model_id: Unique id for the model
        project_id: Unique id for the project the model belongs to
        name: Name of the model
        latest_version_id: Latest Version ID of the model
        model_type: Type of the model
        sub_model_type: Sub type of the model
        description: Description of the model
        directory_id: Unique id for the directory the model belongs to
        created_at: Timestamp of when the model was created
        modified_at: Timestamp of when the model was last modified
    """

    # Attributes: can be accessed or updated directly
    model_id: Optional[str] = None
    project_id: Optional[str] = None
    name: Optional[str] = None
    latest_version_id: Optional[str] = None
    model_type: Optional[str] = None
    sub_model_type: Optional[str] = None
    description: Optional[str] = None
    directory_id: Optional[str] = None

    # Managed attributes: should not be set externally
    created_at: Optional[int] = None
    modified_at: Optional[int] = None

    DATABASE_TYPE: ClassVar[str] = DatabaseType.service
    DATABASE_TABLE: ClassVar[str] = ServiceTableType.model

    """
    Getters and setters
    """

    # Model does not require getters or setters

    """
    Attribute configuration operations
    """

    def validate(self, database_client: Type["DatabaseClient"]):
        """
        Check the validity of existing fields and add missing information if necessary
        Validity check should be performed BEFORE pushing to database

        Args:
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InvalidInputException
        """
        # Existing component validation (model_id is present)
        if self.model_id:
            # Make sure that the project_id is set appropriately, fetch database if necessary
            project_id = self._query(
                database_client=database_client,
                model_id=self.model_id,
                fetchone=True,
            ).project_id

            # If project_id is set, check if it is consistent with database record
            if self.project_id:
                if self.project_id != project_id:
                    raise InvalidInputException(
                        "The model does not belong to the specified project"
                    )

            # Verify if the component id is not alias
            if is_alias(self.model_id):
                raise InvalidInputException("Resolve the alias before usage")

        if self.name:
            # Check for uniqueness of name
            models: dict[str, namedtuple] = {
                model.model_id: model
                for model in self._query(
                    database_client=database_client,
                    project_id=self.project_id,
                    fetchone=False,
                )
            }
            existing_model_names = {
                model.name
                for model in models.values()
                if model.model_id != self.model_id and model.name
            }

            if self.name in existing_model_names:
                raise InvalidInputException(
                    f"A model with name {self.name} already exists"
                )

            # Check for validity of name
            try:
                if (
                    self.model_id not in models
                    or self.name != models[self.model_id].name
                ):
                    validate_filename(self.name)
            except ValidationError as e:
                error_message = re.search(r"(\[\S*]) (.*)", str(e)).group(1)
                raise InvalidInputException(f"Invalid name: {error_message}")

        # Directory validation
        if self.directory_id:
            try:
                # Query the database for the directory information
                directory = database_client.query(
                    database=DatabaseType.service,
                    table=ServiceTableType.directory,
                    directory_id=self.directory_id,
                    project_id=self.project_id,
                    type=COMPONENT_TYPES.model,
                    fetchone=False,
                )
            except NoDirectoryException as e:
                raise InvalidInputException(
                    f"Model cannot be in different directory type"
                )

    @classmethod
    def resolve_alias(
        cls, model_id, project_id, database_client: Type["DatabaseClient"]
    ):
        """
        Convert alias back to the component id, if necessary

        Args:
            model_id (str):
            project_id (str):
            database_client (Type[DatabaseClient]): Database client

        Raises:
            NoProjectAliasException: model_id is a non-existent alias
            InvalidInputException: Project must be provided with an alias
        """
        if not is_alias(model_id):
            return model_id

        if not project_id:
            raise InvalidInputException("Project must be provided with an alias")

        return to_project_component_id(
            database_client=database_client,
            identifier=model_id,
            project_id=project_id,
        )

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
            NoProjectAliasException: model_id is a non-existent alias
            InvalidInputException: Project must be provided with an alias
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
        model_id: str,
        project_id: str = None,
    ):
        """
        Pull model with the given model id from the database

        Args:
            database_client (Type["DatabaseClient"]): Database client
            model_id (str): Model ID
            project_id (str): Project ID

        Returns:
            Model: Model object

        Raises:
            NoModelException: if no model is found with the given model ID
            InvalidInputException: if project id is not provided alongside with alias
        """
        if not model_id:
            raise NoModelException("Model ID must be provided")

        model_id = cls.resolve_alias(
            model_id=model_id,
            project_id=project_id,
            database_client=database_client,
        )

        return cls.from_namedtuple(
            obj_namedtuple=cls._query(
                database_client=database_client,
                model_id=model_id,
                fetchone=True,
            )
        )

    def push(self, database_client: Type["DatabaseClient"]) -> None:
        """
        Push the (potentially updated) model to the database

        Args:
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InavlidInputException: If alias is not resolved
            DatabaseException: If database client is not available
        """
        # Verify if the component id is not alias
        if is_alias(self.model_id):
            raise InvalidInputException("Resolve the alias before usage")

        if not database_client:
            raise DatabaseException("Database client not available")

        self.model_id = self._upsert(
            database_client=database_client,
            model_id=self.model_id,
            name=self.name,
            project_id=self.project_id,
            latest_version_id=self.latest_version_id,
            model_type=self.model_type,
            sub_model_type=self.sub_model_type,
            description=self.description,
            directory_id=self.directory_id,
        )

    def delete(self, database_client: Type["DatabaseClient"]) -> None:
        """
        Delete model with the given model_id from the database and all associated data

        Args:
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InvalidInputException: Resolve the alias before usage
        """
        # Verify if the component id is not alias
        if is_alias(self.model_id):
            raise InvalidInputException("Resolve the alias before usage")

        model = self.pull(
            database_client=database_client,
            model_id=self.model_id,
        )

        # Delete all associated model versions
        ModelVersions.delete(
            database_client=database_client,
            model_id=self.model_id,
        )

        # Delete model from database
        self._delete(database_client=database_client, model_id=model.model_id)

        # Delete model path
        object_store_model_path = os.path.join(model.project_id, model.model_id)

        # Delete model folder with its versions
        ObjectStore.delete_directory(
            bucket=OBJECT_STORE_BUCKET.model, object_store_path=object_store_model_path
        )

        # Delete annotations
        database_client.delete(
            database=DatabaseType.service,
            table=ServiceTableType.annotation,
            component_type=COMPONENT_TYPES.model,
            component_id=model.model_id,
        )

        # Delete aliases
        delete_alias_for_component(
            database_client=database_client, component_id=model.model_id
        )

    """
    Message Format operations
    """

    @classmethod
    def from_proto(cls, obj_proto: PyprModel, database_client: Type["DatabaseClient"]):
        """
        Create a Model from a PyPrModel proto

        Args:
            obj_proto: PyPrModel Proto
            database_client (Type[DatabaseClient]): Database client

        Returns:
            Model: Model object

        Raises:
            NoProjectAliasException: if model_id is a non-existent alias
            InvalidInputException: if name is already taken by another model in the project
            InvalidInputException: if name is not a valid filename
        """

        model_id = obj_proto.model_id if obj_proto.model_id else None
        name = obj_proto.name if obj_proto.name else None
        project_id = obj_proto.project_id if obj_proto.project_id else None
        directory_id = (
            obj_proto.directory.directory_id
            if obj_proto.directory.directory_id is not None
            else ""
        )

        # Resolve alias if needed
        if model_id:
            model_id = cls.resolve_alias(
                model_id=model_id,
                project_id=project_id,
                database_client=database_client,
            )

        # Resolve directory alias
        if directory_id:
            directory_id = cls.resolve_directory_alias(
                directory_id=directory_id,
                project_id=project_id,
                database_client=database_client,
            )

        latest_version_id = (
            obj_proto.latest_version_id
            if obj_proto.latest_version_id is not None
            else ""
        )
        model_type = (
            format_external_model_type_to_model_type(obj_proto.model_type)
            if obj_proto.model_type
            else None
        )
        sub_model_type = obj_proto.sub_model_type if obj_proto.sub_model_type else None
        description = obj_proto.description if obj_proto.description else ""
        created_at = int(obj_proto.created_at) if obj_proto.created_at else None
        modified_at = int(obj_proto.modified_at) if obj_proto.modified_at else None

        model = cls(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            directory_id=directory_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            created_at=created_at,
            modified_at=modified_at,
        )
        model.validate(database_client=database_client)

        return model

    def to_proto(self) -> PyprModel:
        """
        Convert Model to PyPrModel proto

        Returns:
            PyprModel: PyprModel proto representation of the Model
        """
        model_type = (
            format_model_type_to_external_model_type(self.model_type)
            if self.model_type
            else None
        )
        directory = PyprDirectory(
            directory_id=self.directory_id, type=COMPONENT_TYPES.model
        )
        return PyprModel(
            model_id=self.model_id,
            name=self.name,
            project_id=self.project_id,
            latest_version_id=self.latest_version_id,
            model_type=model_type,
            sub_model_type=self.sub_model_type,
            description=self.description,
            directory=directory,
            created_at=str(self.created_at),
            modified_at=str(self.modified_at),
        )

    @classmethod
    def from_namedtuple(
        cls,
        obj_namedtuple: namedtuple,
    ):
        """
        Create a Model from a namedtuple

        Args:
            obj_namedtuple (namedtuple): Model namedtuple

        Returns:
            Model: Model object
        """

        return cls(
            model_id=obj_namedtuple.model_id,
            project_id=obj_namedtuple.project_id,
            name=obj_namedtuple.name,
            latest_version_id=obj_namedtuple.latest_version_id,
            model_type=obj_namedtuple.model_type,
            sub_model_type=obj_namedtuple.sub_model_type,
            directory_id=obj_namedtuple.directory_id,
            description=obj_namedtuple.description,
            created_at=obj_namedtuple.created_at,
            modified_at=obj_namedtuple.modified_at,
        )

    """
    Class utility methods
    """

    @classmethod
    def _upsert(cls, database_client: Type["DatabaseClient"], **kwargs) -> str:
        """
        Upsert a model in the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            **kwargs: Keyword arguments to be passed to the database client

        Returns:
            str: Model ID
        """
        return database_client.upsert(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            **kwargs,
        )

    @classmethod
    def _query(
        cls,
        database_client: Type["DatabaseClient"],
        fetchone: bool = True,
        **kwargs,
    ) -> Union[namedtuple, list[namedtuple]]:
        """
        Get a Model from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            fetchone (bool): Whether to fetch one or many models
            **kwargs: Keyword arguments to be passed to the database client

        Returns:
            namedtuple: Model namedtuple if fetchone is True
            list[namedtuple]: List of Model namedtuples if fetchone is False

        Raises:
            NoModelException: If fetchone and no model is found
        """
        return database_client.query(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            fetchone=fetchone,
            **kwargs,
        )

    @classmethod
    def _delete(cls, database_client: Type["DatabaseClient"], model_id: str) -> None:
        """
        Delete a model from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            model_id (str): Model ID
        """
        database_client.delete(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            model_id=model_id,
        )
