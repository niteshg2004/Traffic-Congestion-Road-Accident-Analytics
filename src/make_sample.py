"""Generate a SYNTHETIC (fake, random) smoke-test file with US-Accidents-like columns.

!! This is NOT real data. Never report numbers produced from it. !!
It only lets you test that the pipeline and dashboard run end to end.
Usage:  python -m src.make_sample
"""
import numpy as np
import pandas as pd

from src import config


def make(n: int = 4000, seed: int = config.RANDOM_SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    centres = [("Los Angeles", "CA", 34.05, -118.24), ("Houston", "TX", 29.76, -95.37),
               ("Miami", "FL", 25.76, -80.19), ("Charlotte", "NC", 35.23, -80.84),
               ("Portland", "OR", 45.52, -122.68), ("Atlanta", "GA", 33.75, -84.39)]
    idx = rng.choice(len(centres), n, p=[.28, .2, .18, .14, .1, .1])
    lat = np.array([centres[i][2] for i in idx]) + rng.normal(0, 0.05, n)
    lng = np.array([centres[i][3] for i in idx]) + rng.normal(0, 0.05, n)
    start = pd.to_datetime("2019-01-01") + pd.to_timedelta(rng.integers(0, 3 * 365 * 24 * 60, n), unit="m")
    weather = rng.choice(["Clear", "Fair", "Mostly Cloudy", "Light Rain", "Heavy Rain",
                          "Fog", "Snow", "Thunderstorm", "Overcast", None],
                         n, p=[.25, .15, .15, .1, .04, .05, .04, .03, .15, .04])
    hour = start.hour.values
    night = (hour < 6) | (hour >= 19)
    bad_wx = np.isin(weather, ["Heavy Rain", "Fog", "Snow", "Thunderstorm"])
    p_sev = 0.12 + 0.08 * night + 0.07 * bad_wx
    severe = rng.random(n) < p_sev
    sev = np.where(severe, rng.choice([3, 4], n, p=[.8, .2]), rng.choice([1, 2], n, p=[.1, .9]))

    def flag(p):
        return rng.choice(["True", "False"], n, p=[p, 1 - p])

    df = pd.DataFrame({
        "ID": [f"A-{i}" for i in range(n)], "Source": "Synthetic", "Severity": sev,
        "Start_Time": start.strftime("%Y-%m-%d %H:%M:%S"),
        "End_Time": (start + pd.to_timedelta(rng.integers(10, 300, n), unit="m")).strftime("%Y-%m-%d %H:%M:%S"),
        "Start_Lat": lat, "Start_Lng": lng, "End_Lat": lat + 0.01, "End_Lng": lng + 0.01,
        "Distance(mi)": rng.gamma(1, 0.5, n).round(2), "Description": "synthetic record",
        "Street": "Main St", "City": [centres[i][0] for i in idx], "County": "Synthetic",
        "State": [centres[i][1] for i in idx], "Zipcode": "00000", "Country": "US",
        "Timezone": "US/Pacific", "Airport_Code": "XXXX",
        "Weather_Timestamp": start.strftime("%Y-%m-%d %H:%M:%S"),
        "Temperature(F)": rng.normal(60, 18, n).round(1),
        "Wind_Chill(F)": rng.normal(58, 18, n).round(1),
        "Humidity(%)": rng.integers(15, 100, n), "Pressure(in)": rng.normal(29.9, 0.3, n).round(2),
        "Visibility(mi)": np.where(bad_wx, rng.uniform(0.2, 4, n), rng.uniform(5, 10, n)).round(1),
        "Wind_Direction": "N", "Wind_Speed(mph)": rng.gamma(2, 3, n).round(1),
        "Precipitation(in)": np.where(bad_wx, rng.uniform(0, 0.5, n), 0).round(2),
        "Weather_Condition": weather,
        "Amenity": flag(.02), "Bump": flag(.002), "Crossing": flag(.1), "Give_Way": flag(.01),
        "Junction": flag(.12), "No_Exit": flag(.005), "Railway": flag(.01),
        "Roundabout": flag(.001), "Station": flag(.03), "Stop": flag(.03),
        "Traffic_Calming": flag(.002), "Traffic_Signal": flag(.2), "Turning_Loop": "False",
        "Sunrise_Sunset": np.where(night, "Night", "Day"),
        "Civil_Twilight": "Day", "Nautical_Twilight": "Day", "Astronomical_Twilight": "Day"})
    # inject imperfections so the cleaning stage has something to do
    for col in ("Temperature(F)", "Visibility(mi)", "Humidity(%)"):
        df.loc[rng.random(n) < 0.03, col] = np.nan
    df.loc[rng.random(n) < 0.01, "City"] = None
    df = pd.concat([df, df.sample(40, random_state=seed)], ignore_index=True)  # duplicate IDs
    return df


if __name__ == "__main__":
    config.SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    make().to_csv(config.SAMPLE_FILE, index=False)
    print(f"Wrote SYNTHETIC sample: {config.SAMPLE_FILE}")
