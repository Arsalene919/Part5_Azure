from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

def main(Mapperinput: dict) -> list:
    try:
        # Blob Storage connection string
        connection_string = os.environ["AzureWebJobsStorage"]

        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Access the blob
        file_name = Mapperinput["file"]
        container_name = Mapperinput.get("container", "default-container")  # Default to a container
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(file_name)

        # Check if blob exists
        if not blob_client.exists():
            raise Exception(f"Blob '{file_name}' not found in container '{container_name}'.")

        # Read file content
        blob_content = blob_client.download_blob().content_as_text()

        # Tokenize file content and return <word, 1> pairs
        result = []
        for line in blob_content.splitlines():
            for word in line.strip().split():
                result.append((word.lower(), 1))

        return result

    except Exception as e:
        # Log the error
        return {"error": str(e)}
