import datetime as dt

import pytest
from pyspark.sql import Row

from src.ml import features as ft
from src.schemas.business_schema import business_schema
from src.schemas.checkin_schema import checkin_schema
from src.schemas.photo_schema import photo_schema
from src.schemas.review_schema import review_schema
from src.schemas.tip_schema import tip_schema

SNAPSHOT = dt.date(2022, 1, 19)


def _business(spark, rows):
    defaults = {
        "business_id": "b",
        "name": "n",
        "address": "a",
        "city": "c",
        "state": "PA",
        "postal_code": "1",
        "latitude": 40.0,
        "longitude": -75.0,
        "stars": 4.0,
        "review_count": 10,
        "is_open": 1,
        "attributes": None,
        "categories": None,
        "hours": None,
    }
    return spark.createDataFrame(
        [Row(**{**defaults, **r}) for r in rows], business_schema
    )


def test_parse_attributes_handles_python_repr_values(spark):
    attrs = {
        "WiFi": "u'free'",
        "Alcohol": "u'none'",
        "NoiseLevel": "None",
        "BikeParking": "True",
        "HasTV": "False",
        "RestaurantsPriceRange2": "2",
        "Ambience": "{'casual': True, u'classy': False, 'divey': None}",
        "BusinessParking": "None",
    }
    df = _business(
        spark,
        [
            {"business_id": "with", "attributes": attrs},
            {
                "business_id": "price-none",
                "attributes": {"RestaurantsPriceRange2": "None"},
            },
            {"business_id": "null"},
        ],
    )
    rows = {r.business_id: r for r in ft.parse_attributes(df).collect()}

    full = rows["with"]
    assert full.wifi == "free"
    assert full.alcohol == "none"  # quoted 'none' is a real level
    assert full.noise_level == "missing"  # bare None is unknown
    assert full.attire == "missing"
    assert full.attr_BikeParking == 1.0
    assert full.attr_HasTV == -1.0
    assert full.attr_GoodForKids == 0.0
    assert full.price_range == 2.0
    assert full.ambience_casual == 1.0
    assert full.ambience_classy == -1.0  # u'classy' key form
    assert full.ambience_divey == 0.0
    assert full.parking_lot == 0.0
    assert full.n_attributes == 8.0

    assert rows["price-none"].price_range is None  # no ANSI cast error
    empty = rows["null"]
    assert empty.n_attributes == 0.0
    assert empty.wifi == "missing"
    assert empty.attr_BikeParking == 0.0


def test_parse_hours(spark):
    hours = {
        "Monday": "8:0-18:30",
        "Tuesday": "0:0-0:0",
        "Friday": "18:0-2:0",
        "Saturday": "garbage",
    }
    df = _business(
        spark,
        [{"business_id": "h", "hours": hours}, {"business_id": "none"}],
    )
    rows = {r.business_id: r for r in ft.parse_hours(df).collect()}

    h = rows["h"]
    assert h.has_hours == 1.0
    # Monday 10.5h + Friday overnight 8h; "0:0-0:0" and garbage excluded.
    assert h.n_open_days == 2.0
    assert h.weekly_open_hours == pytest.approx(18.5)
    assert h.open_late == 1.0
    assert h.opens_early == 0.0
    assert h.hours_zero_format == 1.0

    none = rows["none"]
    assert none.has_hours == 0.0
    assert none.n_open_days == 0.0
    assert none.weekly_open_hours == 0.0
    assert none.open_late == 0.0
    assert none.open_weekend == 0.0


def test_parse_categories(spark):
    df = _business(
        spark,
        [
            {"business_id": "a", "categories": "Restaurants, Pizza"},
            {"business_id": "b", "categories": ""},
            {"business_id": "c"},
        ],
    )
    rows = {r.business_id: r for r in ft.parse_categories(df).collect()}
    assert rows["a"].category_list == ["restaurants", "pizza"]
    assert rows["a"].n_categories == 2.0
    assert rows["b"].category_list == []
    assert rows["c"].category_list == []
    assert rows["c"].n_categories == 0.0


def test_fold_rare_states(spark):
    rows = [{"business_id": f"pa{i}", "state": "PA"} for i in range(3)]
    rows.append({"business_id": "x", "state": "XMS"})
    df = ft.fold_rare_states(_business(spark, rows), min_size=2)
    states = {r.business_id: r.state for r in df.collect()}
    assert states["x"] == "OTHER"
    assert states["pa0"] == "PA"


def test_checkin_aggregates_counts_and_recency(spark):
    df = spark.createDataFrame(
        [("b", "2020-01-01 10:00:00, 2021-12-31 23:00:00")], checkin_schema
    )
    row = ft.checkin_aggregates(df, SNAPSHOT).first()
    assert row.n_checkins == 2
    assert row.last_checkin_date == dt.date(2021, 12, 31)
    assert row.n_checkins_last_year == 1


def test_review_aggregates_never_use_star_ratings(spark):
    review = spark.createDataFrame(
        [
            ("r1", "u", "b", 1.0, "2022-01-19 10:00:00", "bad", 3, 1, 0),
            ("r2", "u", "b", 5.0, "2019-01-01 10:00:00", "great!", 1, 0, 2),
        ],
        review_schema,
    )
    agg = ft.review_aggregates(review, SNAPSHOT)
    assert not [c for c in agg.columns if "star" in c]
    row = agg.first()
    assert row.n_reviews == 2
    assert row.avg_review_useful == 2.0
    assert row.n_reviews_last_year == 1


def test_build_business_features_end_to_end(spark):
    business = _business(
        spark,
        [
            {"business_id": "open", "is_open": 1, "categories": "Food"},
            {"business_id": "closed", "is_open": 0, "stars": 2.5},
        ],
    )
    review = spark.createDataFrame(
        [
            ("r1", "u", "open", 5.0, "2022-01-19 10:00:00", "x", 0, 0, 0),
            ("r2", "u", "closed", 1.0, "2015-01-01 10:00:00", "yy", 2, 0, 0),
        ],
        review_schema,
    )
    tip = spark.createDataFrame(
        [("t", "2021-06-01 10:00:00", 1, "open", "u")], tip_schema
    )
    checkin = spark.createDataFrame(
        [("open", "2021-12-01 10:00:00")], checkin_schema
    )
    photo = spark.createDataFrame(
        [("p1", "open", "", "food"), ("p2", "open", "", "inside")],
        photo_schema,
    )
    df = ft.build_business_features(business, review, tip, checkin, photo)
    rows = {r.business_id: r for r in df.collect()}

    assert rows["closed"].is_closed == 1.0
    assert rows["open"].is_closed == 0.0
    assert rows["closed"].stars == 2.5
    assert rows["open"].days_since_last_review == 0.0
    assert rows["open"].photo_share_food == 0.5
    # No tips / check-ins / photos: zero counts, "as old as the business".
    closed = rows["closed"]
    assert closed.log_n_tips == 0.0
    assert closed.days_since_last_tip == closed.business_age_days
    assert closed.days_since_last_checkin == closed.business_age_days
    assert closed.photo_share_food == 0.0
    assert closed.share_checkins_last_year == 0.0
    ft.check_features(df)
