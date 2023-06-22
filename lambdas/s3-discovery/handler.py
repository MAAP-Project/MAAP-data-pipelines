import json
import os
import re

import boto3


def assume_role(role_arn, session_name):
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
    )
    return creds["Credentials"]


def handler(event, context):
    bucket = event.get("bucket")
    prefix = event.get("prefix", "")
    filename_regex = event.get("filename_regex", None)
    collection = event.get("collection", prefix.rstrip("/"))
    properties = event.get("properties", {})
    cogify = event.pop("cogify", False)

    if role_arn := os.environ.get("DATA_MANAGEMENT_ROLE_ARN"):
        creds = assume_role(role_arn, "maap-data-pipelines_s3-discovery")
        kwargs = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
    s3client = boto3.client("s3", **kwargs)
    s3paginator = s3client.get_paginator("list_objects_v2")
    start_after = event.pop("start_after", None)
    if start_after:
        pages = s3paginator.paginate(
            Bucket=bucket, Prefix=prefix, StartAfter=start_after
        )
    else:
        pages = s3paginator.paginate(Bucket=bucket, Prefix=prefix)

    file_objs_size = 0
    payload = {**event, "cogify": cogify, "objects": []}

    # Propagate forward optional datetime arguments
    date_fields = {}
    if "single_datetime" in event:
        date_fields["single_datetime"] = event["single_datetime"]
    if "start_datetime" in event:
        date_fields["start_datetime"] = event["start_datetime"]
    if "end_datetime" in event:
        date_fields["end_datetime"] = event["end_datetime"]
    if "datetime_range" in event:
        date_fields["datetime_range"] = event["datetime_range"]

    for page in pages:
        if "Contents" not in page:
            raise Exception(f"No files found at s3://{bucket}/{prefix}")
        for obj in page["Contents"]:
            # The limit is advertised at 256000, but we'll preserve some breathing room
            if file_objs_size > 230000:
                payload["start_after"] = start_after
                break
            filename = obj["Key"]
            if filename_regex and not re.match(filename_regex, filename):
                continue
            file_obj = {
                "collection": collection,
                "remote_fileurl": f"s3://{bucket}/{filename}",
                "upload": event.get("upload", False),
                "user_shared": event.get("user_shared", False),
                "ingest": event.get("ingest", True),
                "properties": properties,
                **date_fields,
            }
            if event.get("gdal_config_options"):
                file_obj["gdal_config_options"] = event["gdal_config_options"]
            payload["objects"].append(file_obj)
            file_obj_size = len(json.dumps(file_obj, ensure_ascii=False).encode("utf8"))
            file_objs_size = file_objs_size + file_obj_size
            start_after = filename
    print(payload["objects"][0])
    return payload


if __name__ == "__main__":
    sample_event = {
        "collection": "icesat2-boreal",
        "prefix": "lduncanson/dps_output/run_boreal_biomass_quick_v2_ubuntu/map_boreal_2022_rh_noground_v1/2022/12/05",
        "bucket": "maap-ops-workspace",
        "filename_regex": "^(.*).tif$",
        "discovery": "s3",
        "upload": True,
        "user_shared": True,
    }

    handler(sample_event, {})
