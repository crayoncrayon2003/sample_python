import requests

def test_case2_1():
    response = requests.get('http://localhost:8080/hello')

    assert response.status_code  == 200
    assert response.text == 'hello\n'

if __name__ == "__main__":
    test_case2_1()
