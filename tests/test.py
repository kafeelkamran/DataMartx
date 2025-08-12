# test_quality_checks.py
import pandas as pd
from metrix_repo.analyzers.churn_analyzer import calculate_churn_rate

def test_calculate_churn_rate():
    # Sample test data
    data = {
        'customer_id': [1, 2, 3, 4],
        'status': ['active', 'churned', 'active', 'churned']
    }
    df = pd.DataFrame(data)

    # Expected churn rate = 2 churned out of 4 total = 0.5
    expected_rate = 0.5
    result = calculate_churn_rate(df)

    assert result == expected_rate, f"Expected {expected_rate}, but got {result}"


def test_no_customers():
    df = pd.DataFrame(columns=['customer_id', 'status'])

    try:
        result = calculate_churn_rate(df)
    except ZeroDivisionError:
        result = None

    assert result is None or result == 0
