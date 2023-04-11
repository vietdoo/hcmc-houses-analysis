import boto3

class s3Action:
    def __init__(self) -> None:
        self.s3 = boto3.client("s3")
        self.bucket = "tigerlake"
        pass

    
    def upload(self, file_path = 'text.txt', target_path = 'text.txt') -> None:
        self.s3.upload_file(
            Filename = file_path,
            Bucket = self.bucket,
            Key = target_path
        )
