from cmr import GranuleQuery
import os
import json
import pystac
from pystac.utils import str_to_datetime
from shapely.geometry import shape
from pypgstac import pypgstac
from rio_stac.stac import bbox_to_geom, create_stac_item
import re
import sys

STAC_DB_HOST = os.environ.get("STAC_DB_HOST")
STAC_DB_USER = os.environ.get("STAC_DB_USER")
STAC_DB_PASSWORD = os.environ.get("STAC_DB_PASSWORD")


def create_item(properties, assets, datetime, cog_url, collection):
    """
    Function to create a stac item from a COG using rio_stac
    """
    try:
        rstac = create_stac_item(
            source=cog_url,
            collection=collection,
            input_datetime=datetime,
            properties=properties,
            with_proj=True,
            with_raster=True,
            assets=assets,
        )
        print(rstac.to_dict())
        print("Created item...")
    except Exception as e:
        print(e)
        return f"failed to produce stac item for {cog_url}"

    return rstac


def create_stac_item_with_cmr(event):
    """
    Function to query CMR for metadata and create a stac item
    """
    api = GranuleQuery()

    concept_id = event["granule_id"]
    cmr_json = api.concept_id(concept_id).get(1)[0]

    cog_url = event["s3_filename"]
    collection = event["collection"]

    assets = {}

    for link in cmr_json["links"]:
        if ".he5" in link["href"]:
            name = link["title"] if "title" in link else link["href"]
            assets[name] = pystac.Asset(
                href=link["href"],
                media_type="application/x-hdf5",
                roles=["data"],
                title="hdf image",
            )
    assets["cog"] = pystac.Asset(
        href=cog_url,
        media_type="image/tiff; application=geotiff",
        roles=["data"],
        title="COG",
    )

    dt = str_to_datetime(cmr_json["time_start"])

    stac_item = create_item(
        properties=cmr_json,
        assets=assets,
        datetime=dt,
        cog_url=cog_url,
        collection=collection,
    )
    return stac_item


def create_stac_item_with_regex(event):
    """
    Function to create a STAC item using a user provided regex to parse datetime from a filename
    """

    cog_url = event["s3_filename"]
    collection = event["collection"]
    assets = {}
    assets["cog"] = pystac.Asset(
        href=cog_url,
        media_type="image/tiff; application=geotiff",
        roles=["data"],
        title="COG",
    )
    datetime_regex = re.compile(event["datetime_regex"]["regex"])
    try:
        match = datetime_regex.match(cog_url)
        print(match)
        elements = [match.group(g) for g in event["datetime_regex"]["target_group"]]
        print(elements)
        datestring = "-".join(elements)
        print(datestring)
        dt = str_to_datetime(datestring)
    except Exception as e:
        print(f"Could not parse date string from filename: {cog_url}")
        return e

    stac_item = create_item(
        properties={},
        assets=assets,
        datetime=dt,
        cog_url=cog_url,
        collection=collection,
    )

    return stac_item


def handler(event, context):
    """
    Lambda handler for STAC Collection Item generation

    Arguments:
    event - object with event parameters to be provided in one of 2 formats.
         Format option 1 (with Granule ID defined to retrieve all  metadata from CMR):
        {
           "collection": "OMDOAO3e",
            "s3_filename": "s3://climatedashboard-data/OMDOAO3e/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5.tif",
            "granule_id": "G2205784904-GES_DISC",
        }
        Format option 2 (with regex provided to parse datetime from the filename:
        {
           "collection": "OMDOAO3e",
            "s3_filename": "s3://climatedashboard-data/OMSO2PCA/OMSO2PCA_LUT_SCD_2005.tif",
            "datetime_regex": {
                "regex": "^(.*?)(_)([0-9][0-9][0-9][0-9])(.tif)$",
                # target_group is the group that contains the datetime string when the original filename is matched on the user provided regex
                "target_group": 3
            }
        }

    """
    try:
        if "granule_id" in event:
            if "datetime_regex" in event:
                raise Exception(
                    "Either granule_id or datetime_regex must be provided, not both."
                )
            else:
                # Only granule_id provided, look up in CMR
                stac_item = create_stac_item_with_cmr(event)
        elif "datetime_regex" in event:
            if "granule_id" in event:
                raise Exception(
                    "Either granule_id or datetime_regex must be provided, not both."
                )
            else:
                stac_item = create_stac_item_with_regex(event)
        else:
            raise Exception("Either granule_id or datetime_regex must be provided")
    except Exception as e:
        print(e)
        return

    try:
        stac_dict = stac_item.to_dict()
    except Exception as e:
        print(e)
        return
    print(json.dumps(stac_dict, indent=4, sort_keys=True))

    return {
        "stac_item": stac_dict
    }


if __name__ == "__main__":
    sample_event = {
        "collection": "BMHD_Ida",
        # "s3_filename": "s3://climatedashboard-data/OMDOAO3e/OMI-Aura_L3-OMDOAO3e_2022m0120_v003-2022m0122t021759.he5.tif",
        # "granule_id": "G2205784904-GES_DISC",
        "s3_filename": "s3://climatedashboard-data/BMHD_Ida/BMHD_Ida2021_NO_LA_August9.tif",
        "datetime_regex": {
            "regex": "^(.*?BMHD_Ida)([0-9][0-9][0-9][0-9])(.*?)(_)([A-Za-z]+[0-9])(.tif)$",
            "target_group": [2, 5],
        },
    }

    handler(sample_event, {})