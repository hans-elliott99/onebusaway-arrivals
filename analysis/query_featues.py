import os
import json
import polars as pl
from onebusaway import OnebusawaySDK
from dotenv import load_dotenv
load_dotenv()

STOP_IDS = [
    "1_29278",   # 23rd & Republican, northbound   (43, 48)
    "1_11200",   # Broadway & E Mercer, northbound (49)
]


def get_stop_and_route_references(client, stop_id):
    stop = client.stop.retrieve(stop_id=stop_id)
    stop = json.loads(stop.model_dump_json())["data"]
    route_data = {
        "stop_id": [],
        "route_id": [],
        "agency_id": [],
        "type": [],
        "color": [],
        "description": [],
        "long_name": [],
        "null_safe_short_name": [],
        "short_name": [],
        "text_color": [],
        "url": []
    }
    routes = stop["references"]["routes"]
    for route in routes:
        route_data["stop_id"].append(stop_id)
        route_data["route_id"].append(route["id"])
        route_data["agency_id"].append(route["agency_id"])
        route_data["type"].append(route["type"])
        route_data["color"].append(route["color"])
        route_data["description"].append(route["description"])
        route_data["long_name"].append(route["long_name"])
        route_data["null_safe_short_name"].append(route["null_safe_short_name"])
        route_data["short_name"].append(route["short_name"])
        route_data["text_color"].append(route["text_color"])
        route_data["url"].append(route["url"])

    stop_data = {
        "stop_id": stop["entry"]["id"],
        "lat": stop["entry"]["lat"],
        "lon": stop["entry"]["lon"],
        "name": stop["entry"]["name"],
        "code": stop["entry"]["code"],
        "direction": stop["entry"]["direction"],
        "location_type": stop["entry"]["location_type"],
        "wheelchair_boarding": stop["entry"]["wheelchair_boarding"]
    }
    stop = pl.DataFrame(stop_data)
    routes = pl.DataFrame(route_data)
    return stop, routes


if __name__ == "__main__":
    client = OnebusawaySDK(
        api_key=os.environ.get("ONEBUSAWAY_API_KEY"),
    )
    stop_data = []
    route_data = []
    for stop_id in STOP_IDS:
        stop, routes = get_stop_and_route_references(client, stop_id)
        stop_data.append(stop)
        route_data.append(routes)

    stop_data = pl.concat(stop_data, how="vertical_relaxed")
    route_data = pl.concat(route_data, how="vertical_relaxed")

    stop_data.write_parquet("./data/stops.parquet")
    route_data.write_parquet("./data/routes.parquet")
