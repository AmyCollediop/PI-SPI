from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

objects = client.list_objects("pispi-raw", recursive=True)

for obj in objects:
    client.remove_object("pispi-raw", obj.object_name)

print("✅ Bucket vidé")