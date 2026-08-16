from app.services.matcher import matches_rule

def test_create_rule(client):
    payload = {
        "keyword": "PRICE",
        "dm_message": "Here is the price list: $10."
    }
    response = client.post("/rules", json=payload)
    assert response.status_code == 201
    
    data = response.json()
    assert "rule_id" in data
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Here is the price list: $10."

def test_keyword_matcher():
    # Exact match
    assert matches_rule("PRICE", "PRICE") is True
    # Case-insensitivity
    assert matches_rule("price", "PRICE") is True
    assert matches_rule("PRiCe", "price") is True
    # Substring matching anywhere
    assert matches_rule("Here is the price please", "PRICE") is True
    assert matches_rule("PRICE!!!", "PRICE") is True
    assert matches_rule("some price tags", "price") is True
    
    # Non-matching
    assert matches_rule("PRIC", "PRICE") is False
    assert matches_rule("cost of item", "price") is False
    assert matches_rule("", "price") is False
    assert matches_rule("price", "") is False
