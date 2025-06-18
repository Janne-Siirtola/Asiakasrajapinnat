import requests


def test_authentication():
    """Unauthenticated requests should return 401."""

    url = "https://asiakasrajapinnat.azurewebsites.net/api/config_page"
    params = {"method": "home"}

    resp = requests.get(url, params=params, timeout=10)

    assert resp.status_code == 4011, f"Unauthenticated GET {params['method']} succeeded"
