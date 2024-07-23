import boto3
from botocore.exceptions import ClientError
from logging import Logger


def move_objects(client: boto3.client,
                 logger: Logger,
                 bucket_name: str,
                 source_prefix: str,
                 destination_prefix: str,
                 replace: bool = False) -> None:
    """
    Move all objects from one prefix to another prefix in the same bucket
    :param replace:
    :param client:
    :param logger:
    :param bucket_name:
    :param source_prefix:
    :param destination_prefix:
    :return:
    """

    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if 'Contents' in response:
            total_objects = 0
            copied_objects = 0

            for obj in response['Contents']:

                source_key = obj['Key']
                print(source_key)

                # Create the new key by replacing the source prefix with the destination prefix
                destination_key = source_key.replace(source_prefix, destination_prefix, 1)

                # log
                logger.info(f"Moving object from {source_key} to {destination_key}")

                try:
                    client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': source_key},
                        Key=destination_key
                    )
                    copied_objects += 1
                    logger.info(f"Copied: {source_key} to {destination_key}")
                except ClientError as e:
                    logger.error(f"Failed to copy {source_key}: {str(e)}")

                if replace:
                    client.delete_object(Bucket=bucket_name, Key=source_key)

            logger.info(f"Copy operation completed. Copied {copied_objects} out of {total_objects} objects.")
        else:
            logger.info(f"No objects found in {bucket_name}/{source_prefix}")

    except ClientError as e:
        logger.error(f"Failed to list objects in {bucket_name}/{source_prefix}: {str(e)}")
