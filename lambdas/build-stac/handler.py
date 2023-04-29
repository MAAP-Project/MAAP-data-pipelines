import json
import os
from sys import getsizeof
from typing import Any, Dict, TypedDict, Union
from uuid import uuid4

import smart_open
from utils import events, stac


class S3LinkOutput(TypedDict):
    stac_file_url: str


class StacItemOutput(TypedDict):
    stac_item: Dict[str, Any]


def handler(event: Dict[str, Any], context) -> Union[S3LinkOutput, StacItemOutput]:
    """
    Lambda handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters to be provided in one of 2 formats.
        Format option 1 (with Granule ID defined to retrieve all metadata from CMR):
        {
            "collection": "OMDOAO3e",
            "remote_fileurl": "s3://climatedashboard-data/OMDOAO3e/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5.tif",
            "granule_id": "G2205784904-GES_DISC",
        }
        Format option 2 (with regex provided to parse datetime from the filename:
        {
            "collection": "OMDOAO3e",
            "remote_fileurl": "s3://climatedashboard-data/OMSO2PCA/OMSO2PCA_LUT_SCD_2005.tif",
        }

    """

    EventType = events.CmrEvent if event.get("granule_id") else events.RegexEvent
    parsed_event = EventType.parse_obj(event)
    stac_item = stac.generate_stac(parsed_event).to_dict()

    output: StacItemOutput = {"stac_item": stac_item}

    # Return STAC Item Directly
    if getsizeof(json.dumps(output)) < (256 * 1024):
        return output

    # Return link to STAC Item
    key = f"s3://{os.environ['BUCKET']}/{uuid4()}.json"
    with smart_open.open(key, "w") as file:
        file.write(json.dumps(stac_item))

    return {"stac_file_url": key}


if __name__ == "__main__":
    # sample_event = {
    #     "collection": "GEDI02_A",
    #     "remote_fileurl": "s3://nasa-maap-data-store/file-staging/nasa-map/GEDI02_A___002/2020.12.31/GEDI02_A_2020366232302_O11636_02_T08595_02_003_02_V002.h5",
    #     "granule_id": "G1201782029-NASA_MAAP",
    #     "id": "G1201782029-NASA_MAAP",
    #     "mode": "cmr",
    #     "test_links": None,
    #     "reverse_coords": None,
    #     "asset_name": "data",
    #     "asset_roles": ["data"],
    #     "asset_media_type": "application/x-hdf5",
    # }
    # print(json.dumps(handler(sample_event, {}), indent=2))

    # asset_event = {
    #     "collection": "AfriSAR_UAVSAR_KZ",
    #     "remote_fileurl": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
    #     "granule_id": "G1200110083-NASA_MAAP",
    #     "id": "G1200110083-NASA_MAAP",
    #     "mode": "cmr",
    #     "test_links": None,
    #     "reverse_coords": None,
    #     "asset_name": "data",
    #     "asset_roles": ["data"],
    #     "asset_media_type": {
    #         "vrt": "application/octet-stream",
    #         "bin": "binary/octet-stream",
    #         "hdr": "binary/octet-stream",
    #     },
    #     "assets": {
    #         "bin": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.bin",
    #         "hdr": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.hdr",
    #         "vrt": "s3://nasa-maap-data-store/file-staging/nasa-map/AfriSAR_UAVSAR_KZ___1/uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz.vrt",
    #     },
    #     "product_id": "uavsar_AfriSAR_v1-coreg_fine_lopenp_14043_16008_140_009_160225_kz",
    # }
    # print(json.dumps(handler(asset_event, {}), indent=2))

    regex_event = {
        "collection": "BIOSAR1",
        "remote_fileurl": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_SLC_HH.tiff",
        "granule_id": "G1200110617-ESA_MAAP",
        "id": "G1200110617-ESA_MAAP",
        "mode": "cmr",
        "test_links": None,
        "reverse_coords": True,
        "asset_name": "data",
        "asset_roles": {
            "prj": ["metadata"],
            "dbf": ["data"],
            "shp": ["data"],
            "shx": ["data"],
            "tiff": ["data"],
        },
        "asset_media_type": {
            "prj": "application/octet-stream",
            "dbf": "binary/octet-stream",
            "shp": "binary/octet-stream",
            "shx": "binary/octet-stream",
            "tiff": "image/tiff",
        },
        "assets": {
            "SLC_HH.tiff": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_SLC_HH.tiff",
            "SLC_HV.tiff": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_SLC_HV.tiff",
            "SLC_VH.tiff": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_SLC_VH.tiff",
            "SLC_VV.tiff": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_SLC_VV.tiff",
            "kz.tiff": "https://bmap-catalogue-data.oss.eu-west-0.prod-cloud-ocb.orange-business.com/Campaign_data/biosar1/biosar1_109_kz.tiff",
        },
        "product_id": "biosar1_109",
    }
    print(json.dumps(handler(regex_event, {}), indent=2))
