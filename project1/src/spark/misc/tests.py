from datetime import datetime


def compare_dateprices(dp_a, dp_b, initial=True):
    date_a, date_b = dp_a[0], dp_b[0]

    if initial:
        best_dp = dp_a if date_a <= date_b else dp_b
    else:
        best_dp = dp_a if date_a > date_b else dp_b

    return best_dp


if __name__ == "__main__":
    date1 = datetime.strptime("2000-01-01", "%Y-%m-%d").date()
    date2 = datetime.strptime("2000-01-01", "%Y-%m-%d").date()

    dp1 = (date1, 100)
    dp2 = (date2, 200)

    best_dp = compare_dateprices(dp1, dp2, initial=False)
    print(best_dp)
