import pytest
import pandas as pd
import json
from datetime import date
from unittest.mock import Mock, patch
from eupower_core.scrapes.rte.core import (
    get_token,
    query_generation_byunit,
    query_water_reservoir_balances,
    query_physical_flows,
    download_rte_generation_mix,
    RTE_REGIONS,
)


# Fixtures
@pytest.fixture
def mock_auth_response():
    return {"access_token": "dummy_token", "token_type": "Bearer"}


@pytest.fixture
def mock_credentials():
    return {"client_id": "dummy_id", "client_secret": "dummy_secret"}


@pytest.fixture
def sample_dates():
    return {"start_date": date(2024, 1, 1), "end_date": date(2024, 1, 2)}


# Authentication Tests
def test_get_token_success(mock_credentials, mock_auth_response):
    with patch("requests.post") as mock_post:
        mock_post.return_value.ok = True
        mock_post.return_value.json.return_value = mock_auth_response

        token_type, access_token = get_token(
            mock_credentials["client_id"], mock_credentials["client_secret"]
        )

        assert token_type == "Bearer"
        assert access_token == "dummy_token"
        mock_post.assert_called_once()


def test_get_token_failure(mock_credentials):
    with patch("requests.post") as mock_post:
        mock_post.return_value.ok = False

        with pytest.raises(Exception):
            get_token(mock_credentials["client_id"], mock_credentials["client_secret"])


# Generation Data Tests
def test_query_generation_byunit(mock_auth_response, sample_dates):
    with patch("requests.get") as mock_get:
        # Convert the mock response to JSON string and then to bytes
        mock_response = {
            "actual_generations_per_unit": [
                {
                    "unit": {
                        "name": "UNIT1",
                        "eic_code": "CODE1",
                        "production_type": "NUCLEAR",
                    },
                    "values": [
                        {
                            "start_date": "2024-01-01T00:00:00+01:00",
                            "end_date": "2024-01-01T00:30:00+01:00",
                            "updated_date": "2024-01-01T00:15:00+01:00",
                            "value": 1000,
                        }
                    ],
                }
            ]
        }
        mock_get.return_value.content = json.dumps(mock_response).encode('utf-8')
        mock_get.return_value.raise_for_status = Mock()

        result = query_generation_byunit(
            mock_auth_response["token_type"],
            mock_auth_response["access_token"],
            sample_dates["start_date"],
            sample_dates["end_date"],
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "unit_name" in result.columns
        assert "value" in result.columns

# Physical Flows Tests
def test_query_physical_flows(mock_auth_response, sample_dates):
    with patch("requests.get") as mock_get:
        # Mock response with both import and export flows
        mock_response = {
            "physical_flows": [
                {
                    "receiver_country_name": "France",
                    "sender_country_name": "Germany",
                    "values": [
                        {
                            "start_date": "2024-01-01T00:00:00+01:00",
                            "end_date": "2024-01-01T00:30:00+01:00",
                            "updated_date": "2024-01-01T00:15:00+01:00",
                            "value": 500,
                        }
                    ],
                },
                {
                    "receiver_country_name": "Germany",
                    "sender_country_name": "France",
                    "values": [
                        {
                            "start_date": "2024-01-01T00:00:00+01:00",
                            "end_date": "2024-01-01T00:30:00+01:00",
                            "updated_date": "2024-01-01T00:15:00+01:00",
                            "value": 200,
                        }
                    ],
                }
            ]
        }
        mock_get.return_value.content = json.dumps(mock_response).encode('utf-8')
        mock_get.return_value.ok = True
        mock_get.return_value.raise_for_status = Mock()

        result = query_physical_flows(
            mock_auth_response["token_type"],
            mock_auth_response["access_token"],
            sample_dates["start_date"],
            sample_dates["end_date"],
            "DE",
        )

        assert isinstance(result, pd.DataFrame)
        assert "imports" in result.columns
        assert "exports" in result.columns
        assert "net_imports" in result.columns
        assert len(result) > 0
        # Verify the calculated values
        assert result.iloc[0]["imports"] == 500
        assert result.iloc[0]["exports"] == 200
        assert result.iloc[0]["net_imports"] == 300  # 500 - 200


# RTE Generation Mix Download Tests
def test_download_rte_generation_mix_national():
    with patch("requests.get") as mock_get:
        mock_get.return_value.iter_content.return_value = [b"dummy_content"]
        with patch("zipfile.ZipFile") as mock_zip:
            mock_zip.return_value.__enter__.return_value.namelist.return_value = [
                "file.csv"
            ]
            mock_zip.return_value.__enter__.return_value.read.return_value = (
                b"Date\tValue\n2024-01-01\t100\n"
            )

            result = download_rte_generation_mix(date(2024, 1, 1))

            assert isinstance(result, pd.DataFrame)
            assert not result.empty


def test_download_rte_generation_mix_regional():
    with patch("requests.get") as mock_get:
        mock_get.return_value.iter_content.return_value = [b"dummy_content"]
        with patch("zipfile.ZipFile") as mock_zip:
            mock_zip.return_value.__enter__.return_value.namelist.return_value = [
                "file.csv"
            ]
            mock_zip.return_value.__enter__.return_value.read.return_value = (
                b"Date\tValue\n2024-01-01\t100\n"
            )

            for region in RTE_REGIONS:
                result = download_rte_generation_mix(date(2024, 1, 1), region=region)
                assert isinstance(result, pd.DataFrame)
                assert not result.empty


def test_download_rte_generation_mix_invalid_region():
    with pytest.raises(AssertionError):
        download_rte_generation_mix(date(2024, 1, 1), region="INVALID")
