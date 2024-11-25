"""
TEST MODEL VERSION

Model Version Dataclass test suites

Copyright 2024, Ikigai Labs.
All rights reserved.

"""
import time
import unittest
from unittest import mock

from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Struct
from ikigai_component.components.model_version import ModelVersion
from ikigai_network.client.agents.database import DatabaseAgentClient
from ikigai_network.database.client import DatabaseClient
from ikigai_network.database.message_utils import DatabaseType, ServiceTableType
from ikigai_network.exceptions.not_found.no_model import NoModelException
from ikigai_proto.pypr_pb2 import PyprModelVersion
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
class TestModelVersion(unittest.TestCase):
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
        Test that ModelVersion is initialized correctly
        when only version_id and model_id is provided
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = None
        model_id = "test_model_id"
        hyperparameters = None
        metrics = None
        fold_metrics = None
        created_at = None
        modified_at = None

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model_version = ModelVersion(
            version_id=version_id,
            model_id=model_id,
        )

        ##############
        # Validation #
        ##############
        # Check that model_version object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)

    def test_init_1(self):
        """
        Test that ModelVersion is initialized correctly
        when only version_id is provided
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = None
        model_id = None
        hyperparameters = None
        metrics = None
        fold_metrics = None
        created_at = None
        modified_at = None

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model_version = ModelVersion(version_id=version_id)

        ##############
        # Validation #
        ##############
        # Check that model_version object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)

    def test_init_2(self):
        """
        Test that ModelVersion is initialized correctly
        when all parameters are provided
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {"fold1": "value1"}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        #############
        # Execution #
        #############
        model_version = ModelVersion(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )

        ##############
        # Validation #
        ##############
        # Check that model_version object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)

    """
    Database Operation Tests
    """

    def test_pull_0(self):
        """
        Test that ModelVersion is pulled and created correctly from an existing database entry
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {"fold1": "value1"}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model_version is fetched from database by version_id
        expected_database_model_version_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                fetchone=True,
                version_id=version_id,
                model_id=model_id,
            )
        )

        # Expecting that the following requests are sent to DatabaseAgentClient.query_records
        expected_query_records_calls = [
            mock.call(expected_database_model_version_query_request),
        ]

        ###########
        # Mocking #
        ###########
        # ModelVersion to be returned by mocked database query
        model_version_dict = dict(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model_version metadata
        mocked_database_model_version_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=model_version_dict,
            )
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_version_query_response,
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
            model_version = ModelVersion.pull(
                database_client=DatabaseClient, version_id=version_id, model_id=model_id
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

        # Check that model_version object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)

    def test_pull_1(self):
        """
        Test that ModelVersion can't be pulled when version_id is not provided
        """
        ###############
        # Preparation #
        ###############
        version_id = None

        ##############
        # Validation #
        ##############
        # Check that ModelVersion can't be created
        with self.assertRaises(NoModelException):
            ModelVersion.pull(database_client=DatabaseClient, version_id=version_id)

    def test_push_0(self):
        """
        Test that ModelVersion is pushed correctly to the database
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {"fold1": "value1"}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_version_id = version_id

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model_version is upsert-ed into the database in a push operation
        expected_database_model_version_upsert_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                version_id=version_id,
                version=version,
                model_id=model_id,
                hyperparameters=hyperparameters,
                metrics=metrics,
                fold_metrics=fold_metrics,
            )
        )

        ###########
        # Mocking #
        ###########
        # Mocked database response for model_version upsert
        mocked_database_model_version_upsert_response = (
            create_database_agent_client_response(
                success=True,
                result=version_id,
            )
        )

        # Populate mock database_agent_client.upsert_record object with mock return values
        self.mocked_database_agent_client.upsert_record.return_value = (
            mocked_database_model_version_upsert_response
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
            model_version = ModelVersion(
                version_id=version_id,
                version=version,
                model_id=model_id,
                hyperparameters=hyperparameters,
                metrics=metrics,
                fold_metrics=fold_metrics,
                created_at=created_at,
                modified_at=modified_at,
            )
            model_version.push(database_client=DatabaseClient)

        ##############
        # Validation #
        ##############
        # Check that the correct requests were sent to DatabaseAgentClient
        self.mocked_database_agent_client.upsert_record.assert_called_once_with(
            expected_database_model_version_upsert_request
        )

        # Check that model_version object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)

    def test_delete_0(self):
        """
        Test that ModelVersion is deleted correctly from the database
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {"fold1": "value1"}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model_version is fetched from database by version_id
        expected_database_model_version_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                fetchone=True,
                version_id=version_id,
                model_id=model_id,
            )
        )

        # Expecting that model_version is requested to be deleted from database
        expected_database_model_version_delete_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                version_id=version_id,
                model_id=model_id,
            )
        )

        # Expecting that the following requests are sent to DatabaseAgentClient.query_records
        expected_query_records_calls = [
            mock.call(expected_database_model_version_query_request),
        ]
        # Expecting that the following requests are sent to DatabaseAgentClient.delete_record
        expected_delete_records_calls = [
            mock.call(expected_database_model_version_delete_request),
        ]

        ###########
        # Mocking #
        ###########
        # ModelVersion to be returned by mocked database query
        model_version_dict = dict(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model_version metadata
        mocked_database_model_version_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=model_version_dict,
            )
        )

        # Mocked database response for delete requests
        mocked_database_delete_response = create_database_agent_client_response(
            success=True,
            result=None,
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_version_query_response,
        ]
        # Populate mock database_agent_client.delete_records object with mock return values
        self.mocked_database_agent_client.delete_records.return_value = (
            mocked_database_delete_response
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
            model_version = ModelVersion(
                version_id=version_id,
                model_id=model_id,
            )
            model_version.delete(database_client=DatabaseClient)

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
        self.mocked_database_agent_client.delete_records.assert_has_calls(
            expected_delete_records_calls,
            any_order=True,
        )

    """
    Message Format Operation Tests
    """

    def test_from_proto_0(self):
        """
        Test that model_version is created correctly from proto object
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = None
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Convert dictionaries to Struct
        hyperparameters_struct = Struct()
        ParseDict(hyperparameters, hyperparameters_struct)

        metrics_struct = Struct()
        ParseDict(metrics, metrics_struct)

        model_version_proto = PyprModelVersion(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters_struct,
            metrics=metrics_struct,
            created_at=str(created_at),
            modified_at=str(modified_at),
        )

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        # Requests expected to be sent to DatabaseAgentClient:
        # Expecting that model version is fetched from database by version_id
        expected_database_model_version_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                fetchone=True,
                version_id=version_id,
            )
        )

        # Expecting that model versions in the model are fetched from database to check for duplicates names
        expected_database_project_model_versions_query_request = (
            create_database_agent_client_request(
                database=DatabaseType.service,
                table=ServiceTableType.model_version,
                fetchone=False,
                model_id=model_id,
            )
        )
        expected_query_records_calls = [
            mock.call(expected_database_model_version_query_request),
            mock.call(expected_database_project_model_versions_query_request),
        ]

        ###########
        # Mocking #
        ###########
        # ModelVersion to be returned by mocked database query
        model_version_dict = dict(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model_version metadata
        mocked_database_model_version_query_response_0 = (
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=model_version_dict,
            )
        )

        mocked_database_model_version_query_response_1 = (
            create_database_agent_client_response(
                success=True,
                fetchone=False,
                result=[model_version_dict],
                header=model_version_dict.keys(),
            )
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_version_query_response_0,
            mocked_database_model_version_query_response_1,
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
            model_version = ModelVersion.from_proto(
                obj_proto=model_version_proto,
                database_client=DatabaseClient,
                model_id=model_id,
                project_id="test_project_id",
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

        # Check that the model_version is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)

    def test_to_proto_0(self):
        """
        Test that model_version is converted correctly to proto object
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Convert dictionaries to Struct
        hyperparameters_struct = Struct()
        ParseDict(hyperparameters, hyperparameters_struct)

        metrics_struct = Struct()
        ParseDict(metrics, metrics_struct)

        # Set up expected result for the test
        expected_proto = PyprModelVersion(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters_struct,
            metrics=metrics_struct,
            created_at=str(created_at),
            modified_at=str(modified_at),
        )

        #############
        # Execution #
        #############
        model_version = ModelVersion(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )
        proto = model_version.to_proto()

        ##############
        # Validation #
        ##############
        # Check that the model_version is converted correctly
        self.assertEqual(proto, expected_proto)

    def test_from_namedtuple_0(self):
        """
        Test that ModelVersion is created correctly from a namedtuple object
        provided by database agent client
        """
        ###############
        # Preparation #
        ###############
        version_id = "test_version_id"
        version = "test_version"
        model_id = "test_model_id"
        hyperparameters = {"param1": "value1"}
        metrics = {"metric1": "value1"}
        fold_metrics = {}
        # Created At: Sun Jan 01 2023 04:59:00 GMT+0000
        created_at = 1672549140
        # Modified At: Sun Jan 01 2023 04:59:00 GMT+0000
        modified_at = 1672549140

        # Set up expected result for the test
        expected_version_id = version_id
        expected_version = version
        expected_model_id = model_id
        expected_hyperparameters = hyperparameters
        expected_metrics = metrics
        expected_fold_metrics = fold_metrics
        expected_created_at = created_at
        expected_modified_at = modified_at

        ###########
        # Mocking #
        ###########
        # ModelVersion to be returned by mocked database query
        model_version_dict = dict(
            version_id=version_id,
            version=version,
            model_id=model_id,
            hyperparameters=hyperparameters,
            metrics=metrics,
            fold_metrics=fold_metrics,
            created_at=created_at,
            modified_at=modified_at,
        )

        # Mocked database response with the model_version metadata
        mocked_database_model_version_query_response = (
            create_database_agent_client_response(
                success=True,
                fetchone=True,
                result=model_version_dict,
            )
        )

        # Populate mock database_agent_client.query_records object with mock return values
        self.mocked_database_agent_client.query_records.side_effect = [
            mocked_database_model_version_query_response,
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
            model_version_namedtuple = ModelVersion._query(
                database_client=DatabaseClient, version_id=version_id
            )
            model_version = ModelVersion.from_namedtuple(
                obj_namedtuple=model_version_namedtuple,
            )

        ##############
        # Validation #
        ##############
        # Check that ModelVersion object is created correctly
        self.assertEqual(model_version.version_id, expected_version_id)
        self.assertEqual(model_version.version, expected_version)
        self.assertEqual(model_version.model_id, expected_model_id)
        self.assertEqual(model_version.hyperparameters, expected_hyperparameters)
        self.assertEqual(model_version.metrics, expected_metrics)
        self.assertEqual(model_version.fold_metrics, expected_fold_metrics)
        self.assertEqual(model_version.created_at, expected_created_at)
        self.assertEqual(model_version.modified_at, expected_modified_at)
