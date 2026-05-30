import pytest
from src.main import add, sub, mul, div, branch, get_data

def test_add():
    assert add(2, 3) == 5

def test_sub():
    assert sub(3, 2) == 1

def test_mul():
    assert mul(2, 3) == 6

def test_div():
    assert div(6, 3) == 2
    with pytest.raises(ValueError):
        div(6, 0)

def test_branch():
    assert branch(True,  True ) == True
    assert branch(True,  False) == False
    assert branch(False, True ) == False

def test_get(mocker):
    response_body = {
        "temperature": 10,
        "humidity"   : 20
    }

    mock_response = mocker.Mock()
    mock_response.json.return_value = response_body
    mocker.patch('requests.get', return_value=mock_response)

    result = get_data()
    assert result == response_body
