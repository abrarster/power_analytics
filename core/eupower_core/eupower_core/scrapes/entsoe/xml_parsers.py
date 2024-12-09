import pandas as pd
import xml.etree.ElementTree as ET


def parse_entsoe_generation(xml_string) -> pd.DataFrame:
    # Define the namespace
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

    # Parse XML
    root = ET.fromstring(xml_string)

    # Get document type
    doc_type = root.find(".//ns:type", ns).text

    # Create lists to store data
    timestamps = []
    values = []
    bidding_zones = []
    units = []
    psr_types = []
    doc_types = []
    flow_types = []  # New list to store whether it's generation or consumption

    # Extract time series data
    for time_series in root.findall(".//ns:TimeSeries", ns):
        # Check for bidding zone type
        in_bidding = time_series.find(".//ns:inBiddingZone_Domain.mRID", ns)
        out_bidding = time_series.find(".//ns:outBiddingZone_Domain.mRID", ns)

        if in_bidding is not None:
            bidding_zone = in_bidding.text
            flow_type = "generation"
        elif out_bidding is not None:
            bidding_zone = out_bidding.text
            flow_type = "consumption"
        else:
            bidding_zone = "Unknown"
            flow_type = "unknown"

        unit = time_series.find(".//ns:quantity_Measure_Unit.name", ns).text
        psr_type = time_series.find(".//ns:MktPSRType/ns:psrType", ns).text

        for period in time_series.findall(".//ns:Period", ns):
            start_time = period.find(".//ns:start", ns).text
            resolution = period.find(".//ns:resolution", ns).text
            resolution = resolution.replace("PT", "").replace("M", "min")

            for point in period.findall(".//ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                quantity = float(point.find("ns:quantity", ns).text)

                time = pd.Timestamp(start_time) + pd.Timedelta(resolution) * (
                    position - 1
                )

                timestamps.append(time)
                values.append(quantity)
                bidding_zones.append(bidding_zone)
                units.append(unit)
                psr_types.append(psr_type)
                doc_types.append(doc_type)
                flow_types.append(flow_type)  # Add flow type to each row

    # Create DataFrame with new flow_type column
    df = pd.DataFrame(
        {
            "for_date": timestamps,
            "generation_mw": values,
            "bidding_zone": bidding_zones,
            "unit": units,
            "psr_type": psr_types,
            "doc_type": doc_types,
            "flow_type": flow_types,
        }
    )

    # Sort by timestamp
    df = df.sort_values("for_date").reset_index(drop=True).astype({"for_date": str})
    return df


def parse_entsoe_load(xml_string) -> pd.DataFrame:
    # Define the namespace
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

    # Parse XML
    root = ET.fromstring(xml_string)

    # Create lists to store data
    timestamps = []
    values = []
    metadata = {
        "mRID": [],
        "businessType": [],
        "objectAggregation": [],
        "outBiddingZone_Domain.mRID": [],
        "quantity_Measure_Unit.name": [],
        "curveType": [],
    }

    # Extract time series data
    for time_series in root.findall(".//ns:TimeSeries", ns):
        # Get all metadata from TimeSeries
        current_metadata = {
            "mRID": time_series.find(".//ns:mRID", ns).text,
            "businessType": time_series.find(".//ns:businessType", ns).text,
            "objectAggregation": time_series.find(".//ns:objectAggregation", ns).text,
            "outBiddingZone_Domain.mRID": time_series.find(
                ".//ns:outBiddingZone_Domain.mRID", ns
            ).text,
            "quantity_Measure_Unit.name": time_series.find(
                ".//ns:quantity_Measure_Unit.name", ns
            ).text,
            "curveType": time_series.find(".//ns:curveType", ns).text,
        }

        for period in time_series.findall(".//ns:Period", ns):
            start_time = period.find(".//ns:start", ns).text
            resolution = period.find(".//ns:resolution", ns).text
            resolution = resolution.replace("PT", "").replace("M", "min")

            for point in period.findall(".//ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                quantity = float(point.find("ns:quantity", ns).text)

                time = pd.Timestamp(start_time) + pd.Timedelta(resolution) * (
                    position - 1
                )

                timestamps.append(time)
                values.append(quantity)

                # Add metadata for each point
                for key in metadata:
                    metadata[key].append(current_metadata[key])

    # Create DataFrame with all metadata columns
    df = pd.DataFrame(
        {
            "for_date": timestamps,
            "load_mw": values,
            **metadata,  # Unpack all metadata columns
        }
    )

    # Sort by timestamp
    df = (
        df.sort_values("for_date")
        .reset_index(drop=True)
        .rename(
            columns={
                "mRID": "mrid",
                "businessType": "business_type",
                "objectAggregation": "object_aggregation",
                "outBiddingZone_Domain.mRID": "out_bidding_zone_mrid",
                "quantity_Measure_Unit.name": "quantity_measure_unit",
                "curveType": "curve_type",
            }
        )
    ).astype({"for_date": str})

    return df


def parse_entsoe_cross_border_flows(xml_string):
    # Define the namespace - ENTSOE uses different namespaces for different document types
    namespaces = {
        "gl": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0",
        "tp": "urn:iec62325.351:tc57wg16:451-3:transmissionrightsdocument:2:0",
        "cf": "urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:0",
        "pd": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0",  # Added this namespace
    }

    # Parse XML
    root = ET.fromstring(xml_string)

    # Detect which namespace is being used
    ns_key = next(
        (k for k, v in namespaces.items() if v in root.tag), "pd"
    )  # Changed default to 'pd'
    ns = {ns_key: namespaces[ns_key]}

    # Create lists to store data
    data = {
        "for_date": [],
        "value": [],
        "unit": [],
        "in_domain": [],
        "out_domain": [],
        "doc_type": [],
    }

    # Get document type
    doc_type = root.find(f".//{ns_key}:type", ns).text

    # Extract time series data
    for time_series in root.findall(f".//{ns_key}:TimeSeries", ns):
        # Get common attributes
        unit = time_series.find(f".//{ns_key}:quantity_Measure_Unit.name", ns)
        unit = unit.text if unit is not None else None

        # Try different domain patterns
        in_domain = time_series.find(f".//{ns_key}:in_Domain.mRID", ns)
        if in_domain is None:
            in_domain = time_series.find(f".//{ns_key}:inBiddingZone_Domain.mRID", ns)
        in_domain = in_domain.text if in_domain is not None else None

        out_domain = time_series.find(f".//{ns_key}:out_Domain.mRID", ns)
        if out_domain is None:
            out_domain = time_series.find(f".//{ns_key}:outBiddingZone_Domain.mRID", ns)
        out_domain = out_domain.text if out_domain is not None else None

        for period in time_series.findall(f".//{ns_key}:Period", ns):
            start_time = period.find(f".//{ns_key}:start", ns).text
            resolution = period.find(f".//{ns_key}:resolution", ns).text
            resolution = resolution.replace("PT", "").replace("M", "min")

            for point in period.findall(f".//{ns_key}:Point", ns):
                position = int(point.find(f"{ns_key}:position", ns).text)
                quantity = float(point.find(f"{ns_key}:quantity", ns).text)

                timestamp = pd.Timestamp(start_time) + pd.Timedelta(resolution) * (
                    position - 1
                )

                data["for_date"].append(timestamp)
                data["value"].append(quantity)
                data["unit"].append(unit)
                data["in_domain"].append(in_domain)
                data["out_domain"].append(out_domain)
                data["doc_type"].append(doc_type)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Sort by timestamp
    df = df.sort_values("for_date").reset_index(drop=True).astype({"for_date": str})

    return df


def parse_entsoe_generation_by_unit(xml_string):
    # Parse XML
    root = ET.fromstring(xml_string)

    # Define the correct namespace
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

    data = []

    # Find all TimeSeries elements
    for ts in root.findall(".//ns:TimeSeries", ns):
        # Get metadata with safe extraction
        try:
            # Basic metadata
            unit = ts.find("ns:quantity_Measure_Unit.name", ns).text
            domain = ts.find("ns:inBiddingZone_Domain.mRID", ns).text
            registered_resource = ts.find("ns:registeredResource.mRID", ns).text
            business_type = ts.find("ns:businessType", ns).text

            # PSR Type metadata
            psr_type = ts.find(".//ns:MktPSRType/ns:psrType", ns).text

            # Power System Resources metadata
            psr_mrid = ts.find(".//ns:PowerSystemResources/ns:mRID", ns).text
            psr_name = ts.find(".//ns:PowerSystemResources/ns:name", ns).text

        except AttributeError as e:
            print(f"Failed to extract metadata from TimeSeries: {e}")
            continue

        # Process each Period
        for period in ts.findall(".//ns:Period", ns):
            # Get start time from timeInterval
            start_elem = period.find(".//ns:timeInterval/ns:start", ns)
            resolution_elem = period.find("ns:resolution", ns)

            if start_elem is None or resolution_elem is None:
                print("Missing start or resolution in period")
                continue

            start = pd.Timestamp(start_elem.text)
            resolution = resolution_elem.text.replace("PT", "").replace("M", "min")

            # Process each Point
            for point in period.findall(".//ns:Point", ns):
                try:
                    position = int(point.find("ns:position", ns).text)
                    quantity = float(point.find("ns:quantity", ns).text)
                except (AttributeError, TypeError):
                    print("Failed to extract position or quantity from point")
                    continue

                # Calculate timestamp
                timestamp = start + pd.Timedelta(resolution) * (position - 1)

                data.append(
                    {
                        "for_date": timestamp,
                        "value": quantity,
                        "unit": unit,
                        "domain": domain,
                        "psr_type": psr_type,
                        "business_type": business_type,
                        "registered_resource": registered_resource,
                        "psr_name": psr_name,
                        "psr_mrid": psr_mrid,
                    }
                )

    if not data:
        print("No data found in XML!")
        return None

    return (
        pd.DataFrame(data)
        .sort_values("for_date")
        .reset_index(drop=True)
        .astype({"for_date": str})
    )


def parse_power_plants(xml_string):
    """
    Parse power plant XML data into a pandas DataFrame

    Args:
        xml_string (str): XML string containing power plant data

    Returns:
        pd.DataFrame: DataFrame containing parsed power plant information
    """
    # Parse XML
    root = ET.fromstring(xml_string)

    # Get namespace
    ns = {"ns": root.tag.split("}")[0].strip("{")} if "}" in root.tag else None

    # Define namespace path prefix if namespace exists
    ns_path = "ns:" if ns else ""

    # List to store data
    plants = []

    # Iterate through TimeSeries elements
    for ts in root.findall(f".//{ns_path}TimeSeries", ns):
        try:
            # Extract provider participant info
            provider_elem = ts.find(f"{ns_path}Provider_MarketParticipant", ns)
            provider_mrid = (
                provider_elem.find(f"{ns_path}mRID", ns)
                if provider_elem is not None
                else None
            )

            # Extract base plant info
            plant = {
                "mRID": ts.find(f"{ns_path}mRID", ns).text,
                "business_type": ts.find(f"{ns_path}businessType", ns).text,
                "implementation_date": ts.find(
                    f"{ns_path}implementation_DateAndOrTime.date", ns
                ).text,
                "resource_name": ts.find(f"{ns_path}registeredResource.name", ns).text,
                "resource_mRID": ts.find(f"{ns_path}registeredResource.mRID", ns).text,
                "location": ts.find(
                    f"{ns_path}registeredResource.location.name", ns
                ).text,
                "bidding_zone": ts.find(f"{ns_path}biddingZone_Domain.mRID", ns).text,
                "provider_participant": (
                    provider_mrid.text if provider_mrid is not None else None
                ),
                "control_area_domain": ts.find(
                    f"{ns_path}ControlArea_Domain/{ns_path}mRID", ns
                ).text,
            }

            # Get MktPSRType info
            mkt_psr = ts.find(f"{ns_path}MktPSRType", ns)
            if mkt_psr is not None:
                plant["psr_type"] = mkt_psr.find(f"{ns_path}psrType", ns).text

                # Get voltage limit
                voltage_elem = mkt_psr.find(
                    f"{ns_path}production_PowerSystemResources.highVoltageLimit", ns
                )
                if voltage_elem is not None:
                    plant["voltage_limit"] = float(voltage_elem.text)
                    plant["voltage_unit"] = voltage_elem.get("unit")

                # Get nominal power
                power_elem = mkt_psr.find(
                    f"{ns_path}nominalIP_PowerSystemResources.nominalP", ns
                )
                if power_elem is not None:
                    plant["nominal_power"] = float(power_elem.text)
                    plant["power_unit"] = power_elem.get("unit")

                # Get generating units
                gen_units = mkt_psr.findall(
                    f"{ns_path}GeneratingUnit_PowerSystemResources", ns
                )
                if gen_units:
                    # Extract unit information
                    units = []
                    for unit in gen_units:
                        unit_mrid_elem = unit.find(f"{ns_path}mRID", ns)
                        units.append(
                            {
                                "name": unit.find(f"{ns_path}name", ns).text,
                                "mRID": (
                                    unit_mrid_elem.text
                                    if unit_mrid_elem is not None
                                    else None
                                ),
                                "mRID_coding_scheme": (
                                    unit_mrid_elem.get("codingScheme")
                                    if unit_mrid_elem is not None
                                    else None
                                ),
                                "nominal_power": float(
                                    unit.find(f"{ns_path}nominalP", ns).text
                                ),
                                "power_unit": unit.find(f"{ns_path}nominalP", ns).get(
                                    "unit"
                                ),
                                "psr_type": unit.find(
                                    f"{ns_path}generatingUnit_PSRType.psrType", ns
                                ).text,
                            }
                        )

                    plant["unit_count"] = len(units)
                    plant["total_unit_power"] = sum(u["nominal_power"] for u in units)
                    plant["unit_names"] = [u["name"] for u in units]
                    plant["unit_details"] = units

            plants.append(plant)

        except Exception as e:
            print(f"Error processing TimeSeries: {e}")
            continue

    # Create DataFrame
    df = pd.DataFrame(plants)

    # Convert date column
    if not df.empty and "implementation_date" in df.columns:
        df["implementation_date"] = pd.to_datetime(df["implementation_date"])

    return df


def parse_entsoe_day_ahead_prices(xml_string) -> pd.DataFrame:
    # Define the namespace
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

    # Parse XML
    root = ET.fromstring(xml_string)

    # Create lists to store data
    data = {
        "for_date": [],
        "price": [],
        "currency": [],
        "unit": [],
        "bidding_zone": [],
        "resolution": [],  # Added resolution column
    }

    # Extract time series data
    for time_series in root.findall(".//ns:TimeSeries", ns):
        # Get metadata
        currency = time_series.find("ns:currency_Unit.name", ns).text
        unit = time_series.find("ns:price_Measure_Unit.name", ns).text
        bidding_zone = time_series.find("ns:in_Domain.mRID", ns).text

        for period in time_series.findall(".//ns:Period", ns):
            start_time = period.find("ns:timeInterval/ns:start", ns).text
            resolution = period.find("ns:resolution", ns).text

            # Clean up resolution format (e.g., PT15M -> 15min)
            clean_resolution = resolution.replace("PT", "").replace("M", "min")

            for point in period.findall(".//ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                price = float(point.find("ns:price.amount", ns).text)

                timestamp = pd.Timestamp(start_time) + pd.Timedelta(clean_resolution) * (position - 1)

                data["for_date"].append(timestamp)
                data["price"].append(price)
                data["currency"].append(currency)
                data["unit"].append(unit)
                data["bidding_zone"].append(bidding_zone)
                data["resolution"].append(clean_resolution)  # Add resolution to each row

    # Create DataFrame
    df = pd.DataFrame(data)

    # Sort by timestamp and convert to string format
    df = df.sort_values("for_date").reset_index(drop=True).astype({"for_date": str})

    return df