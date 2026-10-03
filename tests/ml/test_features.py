import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.ml import features as ft
from src.schemas.user_schema import user_schema


def _users(spark, rows):
    defaults = {
        "user_id": "u",
        "name": "n",
        "review_count": 10,
        "yelping_since": "2012-01-19 10:00:00",
        "friends": "None",
        "useful": 0,
        "funny": 0,
        "cool": 0,
        "fans": 0,
        "elite": "",
        "average_stars": 4.0,
        # Every raw compliment column (the schema has funny, the
        # features deliberately do not).
        **{f.name: 0 for f in user_schema if f.name.startswith("compliment_")},
    }
    return spark.createDataFrame(
        [Row(**{**defaults, **r}) for r in rows], user_schema
    )


@pytest.mark.parametrize(
    "raw, years",
    [
        ("", []),
        ("None", []),
        ("2017,2018", ["2017", "2018"]),
        # The raw data stores 2020 as "20,20".
        ("2019,20,20,2021", ["2019", "2020", "2021"]),
        ("20,20", ["2020"]),
    ],
)
def test_elite_years_fixes_the_2020_bug(spark, raw, years):
    df = spark.createDataFrame([(raw,)], "elite string")
    got = df.select(ft.elite_years(F.col("elite")).alias("y")).first()["y"]
    assert sorted(got) == years


def test_friend_count(spark):
    df = spark.createDataFrame(
        [("None",), ("a, b,c",), ("x",), (None,)], "friends string"
    )
    got = [
        r.n
        for r in df.select(
            ft.friend_count(F.col("friends")).alias("n")
        ).collect()
    ]
    assert got == [0, 3, 1, 0]


def test_build_user_features(spark):
    users = _users(
        spark,
        [
            {
                "user_id": "elite",
                "review_count": 100,
                "useful": 50,
                "fans": 9,
                "elite": "2019,20,20",
                "friends": "a,b,c",
                "compliment_hot": 3,
                "compliment_plain": 1,
                "yelping_since": "2012-01-19 10:00:00",
            },
            {
                "user_id": "newbie",
                "review_count": 0,
                "yelping_since": "2022-01-19 08:00:00",
            },
        ],
    )
    rows = {r.user_id: r for r in ft.build_user_features(users).collect()}

    e = rows["elite"]
    assert e.is_elite == 1.0
    assert e.n_elite_years == 2.0
    assert e.log_fans == pytest.approx(2.302585, abs=1e-6)  # log(10)
    assert e.fans == 9
    assert e.useful_per_review == 0.5
    assert e.compliments_per_review == pytest.approx(0.04)
    assert e.log_friends == pytest.approx(1.386294, abs=1e-6)  # log(4)
    assert e.years_on_yelp == pytest.approx(10.0, abs=0.01)

    n = rows["newbie"]
    assert n.is_elite == 0.0
    assert n.log_fans == 0.0
    assert n.years_on_yelp == 0.0  # the snapshot is the latest sign-up
    assert n.useful_per_review == 0.0  # 0 / 0 reviews, no ANSI error
    ft.check_features(ft.build_user_features(users))
