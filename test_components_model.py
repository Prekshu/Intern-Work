"""

TEST MODEL

Model Dataclass test suites

Copyright 2024, Ikigai Labs.
All rights reserved.

"""
import time
import unittest
from unittest import mock

from ikigai_component.components.model import Model
from ikigai_network.client.agents.database import DatabaseAgentClient
from ikigai_network.database.client import DatabaseClient
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.invalid_argument.invalid_input import (
    InvalidInputException,
)
from ikigai_network.exceptions.not_found.no_model import NoModelException
from ikigai_network.utils.message_utils import COMPONENT_TYPES
from ikigai_proto.pypr_pb2 import PyprDirectory, PyprModel
from test_utils.database_agent_client_utils import (
    create_database_agent_client_request,
    create_database_agent_client_response,
)

# Fix a time to make testing deterministic
FIXED_TIME = 0

# Patch time.time to return a fixed time for testing purpose
@mock.patch(
    "time.time", mock.Mock(spec=time.time, autospec=True, return_value=FIXED_TIME)
)
class TestModel(unittest.TestCase):
    def setUp(self):
        """
        Set up mock objects for testing
        """
        self.mocked_database_agent_client = mock.Mock(
            spec=DatabaseAgentClient, autospec=True
        )

    """
    Initialization Tests
    """

    def test_init_0(self):
        """
        Test that Model is initialized correctly
        when only model_id and project_id is provided
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = None
        project_id = "test_project_id"
        latest_version_id = None
        model_type = None
        sub_model_type = None
        description = None
        directory_id = None
        created_at = None
        modified_at = None

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model = Model(
            model_id=model_id,
            project_id=project_id,
        )

        ##############
        # Validation #
        ##############
        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)

    def test_init_1(self):
        """
        Test that Model is initialized correctly
        when only model_id is provided
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = None
        project_id = None
        latest_version_id = None
        model_type = None
        sub_model_type = None
        description = None
        directory_id = None
        created_at = None
        modified_at = None

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model = Model(model_id=model_id)

        ##############
        # Validation #
        ##############
        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)

    def test_init_2(self):
        """
        Test that Model is initialized correctly
        when all parameters are provided
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model = Model(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )

        ##############
        # Validation #
        ##############
        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)

    """
    Database Operation Tests
    """

    def test_pull_0(self):
        """
        Test that Model is pulled and created correctly from an existing database entry
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model is fetched from database by model_id
        expected_database_model_query_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model,
            fetchone=True,
            model_id=model_id,
        )

        # Expecting that the following requests are sent to DatabaseAgentClient.query_records
        expected_query_records_calls = [
            mock.call(expected_database_model_query_request),
        ]

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model metadata
        mocked_database_model_query_response = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response,
        ]
        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            model = Model.pull(database_client=DatabaseClient, model_id=model_id)

        ##############
        # Validation #
        ##############
        # Check that the correct requests were sent to DatabaseAgentClient
        self.assertEqual(
            self.mocked_database_agent_client.query_records.call_count,
            len(expected_query_records_calls),
        )
        self.mocked_database_agent_client.query_records.assert_has_calls(
            expected_query_records_calls
        )

        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)

    def test_pull_1(self):
        """
        Test that Model can't be pulled when model_id is not provided
        """
        ###############
        # Preparation #
        ###############
        model_id = None

        ##############
        # Validation #
        ##############
        # Check that Model can't be created
        with self.assertRaises(NoModelException):
            Model.pull(database_client=DatabaseClient, model_id=model_id)

    def test_push_0(self):
        """
        Test that Model is pushed correctly to the database
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_model_id = model_id

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model is upserted into the database in a push operation
        expected_database_model_upsert_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model,
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
        )

        ###########
        # Mocking #
        ###########
        # Mocked database response for model upsert
        mocked_database_model_upsert_response = create_database_agent_client_response(
            success=True,
            result=model_id,
        )

        # Populate mock database_agent_client.upsert_record object with mock return values
        self.mocked_database_agent_client.upsert_record.return_value = (
            mocked_database_model_upsert_response
        )
        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            model = Model(
                model_id=model_id,
                name=name,
                project_id=project_id,
                latest_version_id=latest_version_id,
                model_type=model_type,
                sub_model_type=sub_model_type,
                description=description,
                directory_id=directory_id,
                created_at=created_at,
                modified_at=modified_at,
            )
            model.push(database_client=DatabaseClient)

        ##############
        # Validation #
        ##############
        # Check that the correct requests were sent to DatabaseAgentClient
        self.mocked_database_agent_client.upsert_record.assert_called_once_with(
            expected_database_model_upsert_request
        )

        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)

    def test_delete_0(self):
        """
        Test that Model is deleted correctly from the database
        along with its related objects
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Versions to be returned by mocked database query
        model_versions_list = [
            dict(
                version_id=f"version_{i}",
                version=f"version_name_{i}",
                model_id=model_id,
                hyperparameters=None,
                metrics=None,
                fold_metrics=None,
                created_at=created_at,
                modified_at=modified_at,
            )
            for i in range(5)
        ]

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model is fetched from database by model_id
        expected_database_model_query_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model,
            fetchone=True,
            model_id=model_id,
        )
        # Expecting that all versions of the model are fetched from the database
        expected_database_versions_query_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model_version,
            model_id=model_id,
            model_ids=None,
            fetchone=False,
        )
        # Expecting that all individual version of the model are fetched from the database
        expected_database_model_version_query_requests = [
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                fetchone=True,
                version_id=model_version["version_id"],
                model_id=model_id,
            )
            for model_version in model_versions_list
        ]
        # Expecting that model is requested to be deleted from database
        expected_database_model_delete_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model,
            model_id=model_id,
        )
        # Expecting that versions associated with the model are requested to be deleted from database
        expected_database_versions_delete_requests = [
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                version_id=model_version["version_id"],
                model_id=model_id,
            )
            for model_version in model_versions_list
        ]
        # Expecting that annotations associated to model are requested to be deleted from database
        expected_database_annotation_delete_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.annotation,
                component_type=COMPONENT_TYPES.model,
                component_id=model_id,
            )
        )
        # Expecting that any project aliases associated to model are requested to be deleted from database
        expected_database_model_alias_delete_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.project_alias,
                component_id=model_id,
            )
        )
        # Expecting that any project aliases associated to model version are requested to be deleted from database
        expected_database_version_alias_delete_request = [
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.project_alias,
                component_id=model_version["version_id"],
            )
            for model_version in model_versions_list
        ]

        # Expecting that the following requests are sent to DatabaseAgentClient.query_records
        expected_query_records_calls = [
            mock.call(expected_database_model_query_request),
            mock.call(expected_database_versions_query_request),
            *[
                mock.call(request)
                for request in expected_database_model_version_query_requests
            ],
        ]
        # Expecting that the following requests are sent to DatabaseAgentClient.delete_record
        expected_delete_records_calls = [
            mock.call(expected_database_model_delete_request),
            *[
                mock.call(request)
                for request in expected_database_versions_delete_requests
            ],
            mock.call(expected_database_annotation_delete_request),
            mock.call(expected_database_model_alias_delete_request),
            *[
                mock.call(request)
                for request in expected_database_version_alias_delete_request
            ],
        ]

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model metadata
        mocked_database_model_query_response = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )
        # Mocked database response with the versions metadata
        mocked_database_versions_query_response = create_database_agent_client_response(
            success=True,
            fetchone=False,
            result=model_versions_list,
            header=model_versions_list[0].keys(),
        )
        # # Mocked database response with the individual model version metadata
        mocked_database_model_version_query_response = [
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=model_version,
            )
            for model_version in model_versions_list
        ]
        # Mocked database response for delete requests
        mocked_database_delete_response = create_database_agent_client_response(
            success=True,
            result=None,
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response,
            mocked_database_versions_query_response,
            *mocked_database_model_version_query_response,
        ]
        # Populate mock database_agent_client.delete_records object with mock return values
        self.mocked_database_agent_client.delete_records.side_effect = [
            mocked_database_delete_response
            for _ in range(len(expected_delete_records_calls))
        ]
        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            Model(model_id=model_id).delete(database_client=DatabaseClient)

        ##############
        # Validation #
        ##############
        # Check that the correct requests were sent to DatabaseAgentClient
        self.assertEqual(
            self.mocked_database_agent_client.query_records.call_count,
            len(expected_query_records_calls),
        )
        self.mocked_database_agent_client.query_records.assert_has_calls(
            expected_query_records_calls
        )
        self.assertEqual(
            self.mocked_database_agent_client.delete_records.call_count,
            len(expected_delete_records_calls),
        )
        self.mocked_database_agent_client.delete_records.assert_has_calls(
            expected_delete_records_calls,
            any_order=True,
        )

    """
    Component Operation Tests
    """

    def test_name_0(self):
        """
        Test that model name is not validated if not changed
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test/model>name*"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_name = name

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            project_id=project_id,
            name=name,
        )  # Only these fields may be accessed by __init__ for this test

        # Mocked directory response
        directory_dict = dict(
            directory_id=directory_id,
            type=COMPONENT_TYPES.model,
        )

        # Mocked database response with all (1) models in the project
        mocked_database_model_query_response_0 = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )

        mocked_database_model_query_response_1 = create_database_agent_client_response(
            success=True,
            fetchone=False,
            result=[model_dict],
            header=model_dict.keys(),
        )

        # Mocked database response with the directory metadata
        mocked_database_directory_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=directory_dict,
            )
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response_0,
            mocked_database_model_query_response_1,
            mocked_database_directory_query_response,
        ]

        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            model = Model(
                model_id=model_id,
                name=name,
                project_id=project_id,
                latest_version_id=latest_version_id,
                model_type=model_type,
                sub_model_type=sub_model_type,
                description=description,
                directory_id=directory_id,
                created_at=created_at,
                modified_at=modified_at,
            )
            model.validate(database_client=DatabaseClient)

        ##############
        # Validation #
        ##############
        # Check that the name is the expected value
        self.assertEqual(model.name, expected_name)

    def test_name_1(self):
        """
        Test that model name is validated if changed
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model_name"
        new_name = "test/model>name*"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            project_id=project_id,
            name=name,
        )  # Only these fields may be accessed by __init__ for this test

        # Mocked database response with all (1) models in the project
        mocked_database_model_query_response_0 = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )

        mocked_database_model_query_response_1 = create_database_agent_client_response(
            success=True,
            fetchone=False,
            result=[model_dict],
            header=model_dict.keys(),
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response_0,
            mocked_database_model_query_response_1,
        ]

        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        # Expect a ValueError to be raised
        with self.assertRaises(InvalidInputException):
            with mock.patch(
                "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
                self.mocked_database_agent_client,
            ):
                model = Model(
                    model_id=model_id,
                    name=name,
                    project_id=project_id,
                    latest_version_id=latest_version_id,
                    model_type=model_type,
                    sub_model_type=sub_model_type,
                    description=description,
                    directory_id=directory_id,
                    created_at=created_at,
                    modified_at=modified_at,
                )
                model.name = new_name
                model.validate(database_client=DatabaseClient)

    def test_name_2(self):
        """
        Test that model name is validated if it is a new model
        """
        ###############
        # Preparation #
        ###############
        model_id = None
        name = "test/model>name*"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        ###########
        # Mocking #
        ###########
        # Mocked database response with all (0) models in the project
        mocked_database_project_models_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=False,
                result=[],
                header=["model_id", "project_id", "name"],
            )
        )

        # Populate mock database_agent_client.query_records object with mocked return values
        self.mocked_database_agent_client.query_records.return_value = (
            mocked_database_project_models_query_response
        )
        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        # Expect a ValueError to be raised
        with self.assertRaises(InvalidInputException):
            with mock.patch(
                "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
                self.mocked_database_agent_client,
            ):
                model = Model(
                    model_id=model_id,
                    name=name,
                    project_id=project_id,
                    latest_version_id=latest_version_id,
                    model_type=model_type,
                    sub_model_type=sub_model_type,
                    description=description,
                    directory_id=directory_id,
                    created_at=created_at,
                    modified_at=modified_at,
                )
                model.validate(database_client=DatabaseClient)

    """
    Message Format Operation Tests
    """

    def test_from_proto_0(self):
        """
        Test that model is created correctly from proto object
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140
        model_proto = PyprModel(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory=PyprDirectory(
                directory_id=directory_id, type=COMPONENT_TYPES.model
            ),
            created_at=str(created_at),
            modified_at=str(modified_at),
        )

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model is fetched from database by model_id
        expected_database_model_query_request = create_database_agent_client_request(
            database=DatabaseType.service,
            table=ServiceTableType.model,
            fetchone=True,
            model_id=model_id,
        )

        # Expecting that models in the project are fetched from database to check for duplicates names
        expected_database_project_models_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model,
                fetchone=False,
                project_id=project_id,
            )
        )

        # Expecting that directory is fetched from database by directory_id
        expected_database_directory_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.directory,
                directory_id=directory_id,
                project_id=project_id,
                type=COMPONENT_TYPES.model,
                fetchone=False,
            )
        )
        expected_query_records_calls = [
            mock.call(expected_database_model_query_request),
            mock.call(expected_database_project_models_query_request),
            mock.call(expected_database_directory_query_request),
        ]

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model metadata
        mocked_database_model_query_response_0 = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )

        mocked_database_model_query_response_1 = create_database_agent_client_response(
            success=True,
            fetchone=False,
            result=[model_dict],
            header=model_dict.keys(),
        )

        # Directory to be returned by mocked database query
        directory_dict = dict(
            directory_id=directory_id,
            name="test_directory",
            type=COMPONENT_TYPES.model,
            project_id=project_id,
            user_id="test_user_id",
            parent_id=None,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the directory metadata
        mocked_database_directory_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=False,
                result=[directory_dict],
                header=directory_dict.keys(),
            )
        )
        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response_0,
            mocked_database_model_query_response_1,
            mocked_database_directory_query_response,
        ]

        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            model = Model.from_proto(
                obj_proto=model_proto,
                database_client=DatabaseClient,
            )

        ##############
        # Validation #
        ##############
        # Check that the correct requests were sent to DatabaseAgentClient
        self.assertEqual(
            self.mocked_database_agent_client.query_records.call_count,
            len(expected_query_records_calls),
        )
        self.mocked_database_agent_client.query_records.assert_has_calls(
            expected_query_records_calls
        )

        # Check that the model is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)

    def test_to_proto_0(self):
        """
        Test that model is converted correctly to proto object
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_model_proto = PyprModel(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory=PyprDirectory(
                directory_id=directory_id, type=COMPONENT_TYPES.model
            ),
            created_at=str(created_at),
            modified_at=str(modified_at),
        )

        #############
        # Execution #
        #############
        model = Model(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )
        model_proto = model.to_proto()

        ##############
        # Validation #
        ##############
        # Check that the model is created correctly
        self.assertEqual(model_proto, expected_model_proto)

    def test_from_namedtuple_0(self):
        """
        Test that model is created correctly from a namedtuple object
        provided by database agent client
        """
        ###############
        # Preparation #
        ###############
        model_id = "test_model_id"
        name = "test_model"
        project_id = "test_project_id"
        latest_version_id = "test_latest_version_id"
        model_type = "test_model_type"
        sub_model_type = "test_sub_model_type"
        description = "test_description"
        directory_id = "test_directory_id"
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_model_id = model_id
        expected_name = name
        expected_project_id = project_id
        expected_latest_version_id = latest_version_id
        expected_model_type = model_type
        expected_sub_model_type = sub_model_type
        expected_description = description
        expected_directory_id = directory_id
        expected_created_at = created_at
        expected_modified_at = modified_at

        ###########
        # Mocking #
        ###########
        # Model to be returned by mocked database query
        model_dict = dict(
            model_id=model_id,
            name=name,
            project_id=project_id,
            latest_version_id=latest_version_id,
            model_type=model_type,
            sub_model_type=sub_model_type,
            description=description,
            directory_id=directory_id,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model metadata
        mocked_database_model_query_response = create_database_agent_client_response(
            success=True,
            fetchone=True,
            result=model_dict,
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_query_response,
        ]
        # Seal the mock object to catch unexpected calls
        mock.seal(self.mocked_database_agent_client)

        #############
        # Execution #
        #############
        with mock.patch(
            "ikigai_network.database.client.DATABASE_AGENT_CLIENT",
            self.mocked_database_agent_client,
        ):
            model_namedtuple = Model._query(
                database_client=DatabaseClient, model_id=model_id
            )
            model = Model.from_namedtuple(obj_namedtuple=model_namedtuple)

        ##############
        # Validation #
        ##############
        # Check that model object is created correctly
        self.assertEqual(model.model_id, expected_model_id)
        self.assertEqual(model.project_id, expected_project_id)
        self.assertEqual(model.name, expected_name)
        self.assertEqual(model.latest_version_id, expected_latest_version_id)
        self.assertEqual(model.model_type, expected_model_type)
        self.assertEqual(model.sub_model_type, expected_sub_model_type)
        self.assertEqual(model.description, expected_description)
        self.assertEqual(model.directory_id, expected_directory_id)
        self.assertEqual(model.created_at, expected_created_at)
        self.assertEqual(model.modified_at, expected_modified_at)
