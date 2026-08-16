def matches_rule(comment_text: str, keyword: str) -> bool:
    """
    Checks if a comment text matches a rule's keyword.
    Matching is:
    - case-insensitive
    - substring matching anywhere in comment text
    """
    if not comment_text or not keyword:
        return False
    return keyword.lower() in comment_text.lower()
