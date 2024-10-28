from datetime import date, timedelta


def get_relative_date_range(days_back: int, days_forward: int, freq="D") -> list[str]:
    """
    Get a list of dates relative to today's date.

    Args:
        days_back (int): Number of days to go back from today's date.
        days_forward (int): Number of days to go forward from today's date.
        freq (str): Frequency of the dates. Defaults to "D" for daily.
    Returns:
        list[str]: List of dates as strings.
    """
    if freq != "D":
        raise NotImplementedError(f"Frequency {freq} not implemented")

    start_date = date.today() - timedelta(days=days_back)
    end_date = date.today() + timedelta(days=days_forward)
    dates = [
        (start_date + timedelta(days=x))
        for x in range((end_date - start_date).days + 1)
    ]
    return [d.strftime("%Y-%m-%d") for d in dates]
