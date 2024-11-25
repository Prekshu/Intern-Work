"""

MODEL VERSION DATACLASS

Dataclass for Model Version

Copyright 2024, Ikigai Labs.
All rights reserved.

"""

import re
from collections import namedtuple
from dataclasses import dataclass
from typing import ClassVar, Optional, Type, Union

from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from ikigai_component.alias.alias_utils import (
    delete_alias_for_component,
    is_alias,
    to_project_component_id,
)
from ikigai_model.utils.format_utils import format_hyperparameters, format_metrics
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.internal.database import DatabaseException
from ikigai_network.exceptions.invalid_argument.invalid_input import (
    InvalidInputException,
)
from ikigai_network.exceptions.not_found.no_model import NoModelException
from ikigai_proto.pypr_pb2 import PyprModelVersion
from pathvalidate import ValidationError, validate_filename

# To prevent database client from causing a circular dependency and satisfy F821
if False:
    from ikigai_network.database.client import DatabaseClient


@dataclass
class ModelVersion:
    """
    ModelVersion dataclass

    Class containing information about a Model Version

    Fields:
        model_id: Unique id for the model
        version: Version of the model
        version_id: Unique id for the version of the model
        hyperparameters: Hyperparameters of a particular version of the model
        metrics: Metrics of a particular version of the model
        fold_metrics: Fold Metrics of a particular version of the model
        created_at: Timestamp of when the model version was created
        modified_at: Timestamp of when the model version was last modified
    """

    # Attributes: can be accessed or updated directly
    model_id: Optional[str] = None
    version: Optional[str] = None
    version_id: Optional[str] = None
    hyperparameters: Optional[dict] = None
    metrics: Optional[dict] = None
    fold_metrics: Optional[dict] = None

    # Managed attributes: should not be set externally
    created_at: Optional[int] = None
    modified_at: Optional[int] = None

    DATABASE_TYPE: ClassVar[str] = DatabaseType.service
    DATABASE_TABLE: ClassVar[str] = ServiceTableType.model_version

    """
    Getters and setters
    """

    # Model Version does not require getters or setters

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
        # Existing component validation (version_id is present)
        if self.version_id:
            # Make sure that the model_id is set appropriately, fetch database if necessary
            model_id = self._query(
                database_client=database_client,
                version_id=self.version_id,
                fetchone=True,
            ).model_id

            # If model_id is set, check if it is consistent with database record
            if self.model_id and self.model_id != model_id:
                raise InvalidInputException(
                    "The model version does not belong to the specified model"
                )

            # Verify if the component id is not alias
            if is_alias(self.version_id):
                raise InvalidInputException("Resolve the alias before usage")

        if self.version:
            # Check for uniqueness of name
            model_versions: dict[str, namedtuple] = {
                model_version.version_id: model_version
                for model_version in self._query(
                    database_client=database_client,
                    model_id=self.model_id,
                    fetchone=False,
                )
            }

            existing_versions = {
                model_version.version
                for model_version in model_versions.values()
                if model_version.version_id != self.version_id and model_version.version
            }

            if self.version in existing_versions:
                raise InvalidInputException(
                    f"A model version {self.version} already exists"
                )

            # Check for validity of version
            try:
                if (
                    self.version_id not in model_versions
                    or self.version != model_versions[self.version_id].version
                ):
                    validate_filename(self.version)
            except ValidationError as e:
                error_message = re.search(r"(\[\S*]) (.*)", str(e)).group(1)
                raise InvalidInputException(f"Invalid Version: {error_message}")

    @classmethod
    def resolve_alias(
        cls, version_id, project_id, database_client: Type["DatabaseClient"]
    ):
        """
        Convert alias back to the component id, if necessary

        Args:
            version_id (str):
            project_id (str):
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InvalidInputException: 'Project must be provided with an alias'
        """
        if not is_alias(version_id):
            return version_id

        if not project_id:
            raise InvalidInputException("Project must be provided with an alias")

        return to_project_component_id(
            database_client=database_client,
            identifier=version_id,
            project_id=project_id,
        )

    """
    Database operations
    """

    @classmethod
    def pull(
        cls,
        database_client: Type["DatabaseClient"],
        version_id: str,
        model_id: str = None,
        project_id: str = None,
    ):
        """
        Pull model version with the given version ID from the database

        Args:
            database_client (Type["DatabaseClient"]): Database client
            version_id (str): Version ID
            project_id (str): Project ID

        Returns:
            Model Version: Model Version object

        Raises:
            NoModelException: if no model version is found with the given version ID
            InvalidInputException: if project id is not provided alongside with alias
        """
        if not version_id:
            raise NoModelException("Version ID must be provided")

        version_id = cls.resolve_alias(
            version_id=version_id,
            project_id=project_id,
            database_client=database_client,
        )

        return cls.from_namedtuple(
            obj_namedtuple=cls._query(
                database_client=database_client,
                version_id=version_id,
                model_id=model_id,
                fetchone=True,
            ),
        )

    # Push function is currently not in use anywhere
    def push(self, database_client: Type["DatabaseClient"]) -> None:
        """
        Push the (potentially updated) model version to the database

        Args:
            database_client (Type[DatabaseClient]): Database client

        Raises:
            InavlidInputException: If alias is not resolved
            DatabaseException: If database client is not available
        """

        # Verify if the component id is not alias
        if is_alias(self.version_id):
            raise InvalidInputException("Resolve the alias before usage")

        if not database_client:
            raise DatabaseException("Database client not available")

        self.version_id = self._upsert(
            database_client=database_client,
            version_id=self.version_id,
            version=self.version,
            model_id=self.model_id,
            hyperparameters=self.hyperparameters,
            metrics=self.metrics,
            fold_metrics=self.fold_metrics,
        )

    def delete(self, database_client: Type["DatabaseClient"]) -> None:
        """
        Delete version with the given version id from the database and all associated data

        Args:
            database_client (Type[DatabaseClient]): Database client
        """
        # Verify if the component id is not alias
        if is_alias(self.version_id):
            raise InvalidInputException("Resolve the alias before usage")

        model_version = self.pull(
            database_client=database_client,
            version_id=self.version_id,
            model_id=self.model_id,
        )

        # Delete model version from database
        self._delete(
            database_client=database_client,
            version_id=model_version.version_id,
            model_id=model_version.model_id,
        )

        # Delete aliases
        delete_alias_for_component(
            database_client=database_client, component_id=model_version.version_id
        )

    """
    Message Format operations
    """

    @classmethod
    def from_proto(
        cls,
        obj_proto: PyprModelVersion,
        database_client: Type["DatabaseClient"],
        model_id: str,
        project_id: str,
    ):
        """
        Create a ModelVersion from a proto

        Args:
            obj_proto: Proto object
            database_client (Type[DatabaseClient]): Database client

        Returns:
            ModelVersion: ModelVersion object

        Raises:
            InvalidInputException: If some other version already has the same name
            NoProjectAliasException: if version id is a non-existent alias
        """
        version_id = obj_proto.version_id if obj_proto.version_id else None
        version = obj_proto.version if obj_proto.version else None

        # Resolve alias if needed
        if version_id:
            version_id = cls.resolve_alias(
                version_id=version_id,
                project_id=project_id,
                database_client=database_client,
            )

        hyperparameters = (
            MessageToDict(obj_proto.hyperparameters)
            if obj_proto.hyperparameters
            else None
        )
        metrics = MessageToDict(obj_proto.metrics) if obj_proto.metrics else None
        created_at = int(obj_proto.created_at) if obj_proto.created_at else None
        modified_at = int(obj_proto.modified_at) if obj_proto.modified_at else None

        model_version = cls(
            model_id=model_id,
            version=version,
            version_id=version_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            created_at=created_at,
            modified_at=modified_at,
        )
        model_version.validate(database_client=database_client)

        return model_version

    def to_proto(self, apply_formatting: bool = True):
        """
        Convert ModelVersion to proto

        Args:
            apply_formatting: bool variable which if True, formats hyperparameters and metrics

        Returns:
            Proto object
        """

        hyperparameters = (
            format_hyperparameters(self.hyperparameters)
            if apply_formatting
            else ParseDict(self.hyperparameters, Struct())
        )
        metrics = (
            format_metrics(self.metrics)
            if apply_formatting
            else ParseDict(self.metrics, Struct())
        )

        return PyprModelVersion(
            version_id=self.version_id,
            model_id=self.model_id,
            version=self.version,
            hyperparameters=hyperparameters,
            metrics=metrics,
            created_at=str(self.created_at),
            modified_at=str(self.modified_at),
        )

    @classmethod
    def from_namedtuple(
        cls,
        obj_namedtuple: namedtuple,
    ):
        """
        Create a Model Version from a namedtuple

        Args:
            obj_namedtuple (namedtuple): Model Version namedtuple

        Returns:
            Model Version: Model Version object
        """

        return cls(
            model_id=obj_namedtuple.model_id,
            version=obj_namedtuple.version,
            version_id=obj_namedtuple.version_id,
            hyperparameters=obj_namedtuple.hyperparameters,
            metrics=obj_namedtuple.metrics,
            fold_metrics=obj_namedtuple.fold_metrics,
            created_at=obj_namedtuple.created_at,
            modified_at=obj_namedtuple.modified_at,
        )

    """
    Class utility methods
    """

    @classmethod
    def _upsert(cls, database_client: Type["DatabaseClient"], **kwargs) -> str:
        """
        Upsert a model version in the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            **kwargs: Keyword arguments to be passed to the database client

        Returns:
            str: version ID
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
        Get a model version from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            fetchone (bool): Whether to fetch one or many version
            **kwargs: Keyword arguments to be passed to the database client

        Returns:
            namedtuple: model version namedtuple if fetchone is True
            list[namedtuple]: List of Model Verison namedtuples if fetchone is False

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
    def _delete(
        cls, database_client: Type["DatabaseClient"], version_id: str, model_id: str
    ) -> None:
        """
        Delete a model version from the database

        Args:
            database_client (Type[DatabaseClient]): Database client
            version_id (str): Version ID
        """
        database_client.delete(
            database=cls.DATABASE_TYPE,
            table=cls.DATABASE_TABLE,
            version_id=version_id,
            model_id=model_id,
        )
