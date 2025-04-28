import os
import requests
import functools
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result

class CustomRequests(requests.Session):
    def __init__(self):
        super().__init__()

    def get(self, url, **kwargs):
        retry_config = kwargs.pop("retry", {})
        codes = retry_config.get("codes", (500, 502, 503, 504))
        attempts = retry_config.get("attempts", 3)
        wait = retry_config.get("wait", 1)

        retry_decorator = functools.partial(
            retry,
            retry=retry_if_result(lambda r: r is not None and r.status_code in codes),
            stop=stop_after_attempt(attempts),
            wait=wait_fixed(wait),
            reraise=True
        )

        @retry_decorator()
        def send_request():
            response = requests.Session.get(self, url, **kwargs)
            print(f"Response status code: {response.status_code}")
            return response

        response = send_request()
        response.save_local = lambda directory: self.save_local(response, directory)
        return response

    def __get_extension(self, content_type):
        extension_map = {
            "application/json": "json",
            "application/xml": "xml",
            "text/xml": "xml",
            "text/csv": "csv",
            "text/plain": "txt",
            "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "application/zip": "zip",
            "application/x-zip-compressed": "zip",
        }
        if content_type.startswith("image/") or content_type.startswith("audio/") or content_type.startswith("video/"):
            return content_type.split("/")[-1]
        return extension_map.get(content_type, "bin")

    def save_local(self, response, directory, filename="data"):
        content_type = response.headers.get("Content-Type", "").lower().split(";")[0]
        ext = self.__get_extension(content_type)
        filefullname = f"{filename}.{ext}"

        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, filefullname)
        with open(file_path, "wb") as f:
            f.write(response.content)

        print("save local")
        return [directory, filefullname]
