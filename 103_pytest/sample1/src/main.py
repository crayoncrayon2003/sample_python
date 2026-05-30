def add(a, b):
    return a + b

def sub(a, b):
    return a - b

def mul(a, b):
    return a * b

def div(a, b):
    return a / b  # Intentional division by zero

def branch(a, b):
    if (a == True) and (b == True):
        return True
    else:
        return False

def get_data():
    import requests
    response = requests.get("http://localhost:8080")
    return response.json()

