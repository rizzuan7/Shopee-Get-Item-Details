import pyshopee
from datetime import datetime, timedelta, time
import pytz
import pandas as pd
import json
from itertools import zip_longest
import csv
from csv import writer
import pymongo
from pymongo import MongoClient
from pymongo import errors as MongoErrors
from pymongo.errors import ConnectionFailure

# The maximum date range that may be specified with the update_time_from and update_time_to fields is 15 days.


def dt_to_timestamp(dt):
    """
    Convert datetime to timestamp
    """
    if type(dt) == str:
        dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone("UTC"))
        ts = dt.timestamp()
        return int(ts)
    elif type(dt) == datetime:
        dt = dt.astimezone(pytz.utc)
        ts = dt.timestamp()
        return int(ts)

def timestamp_to_dt(ts):
    """
    Convert timestamp to datetime
    """
    if type(ts) == float or type(ts) == int:
        unaware_dt = datetime.fromtimestamp(ts)
        str_dt = unaware_dt.strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.strptime(str_dt, "%Y-%m-%d %H:%M:%S")
    else:
        dt = ts
    return dt

def parse_dt(dt, country):
    if country == "MY" or country == "Nike-MY":
        local_tz = pytz.timezone("Asia/Kuala_Lumpur")
    elif country == "SG" or country == "Nike-SG":
        local_tz = pytz.timezone("Asia/Singapore")
    elif country == "VN":
        local_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    elif country == "PH" or country == "Nike-PH":
        local_tz = pytz.timezone("Asia/Manila")
    elif country == "TH" or country == "Nike-TH":
        local_tz = pytz.timezone("Asia/Bangkok")
    elif country == "ID" or country == "Nike-ID":
        local_tz = pytz.timezone("Asia/Jakarta")
    else:
        local_tz = pytz.timezone("Asia/Kuala_Lumpur")

    dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    dt = local_tz.localize(dt)
    return dt

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue )

def GetItemList(update_time_from, update_time_to, offset, limit):
    """
    Shopee GetItemsList API
    """

    response = client.item.get_item_list(
        update_time_from = update_time_from,
        update_time_to = update_time_to,
        pagination_offset = int(offset),
        pagination_entries_per_page = int(limit)
    )
    
    if "more" in response:

        return json.dumps(
            {
                "status": 200,
                "items": response["items"], 
                "more": response["more"], 
                "total": response["total"],
                "update_time_from": update_time_from,
                "update_time_to": update_time_to
            }
        )

    else:
        return json.dumps({"status":405, "msg":response})


# def GetItemDetail(update_time_from, update_time_to, item_id):
#     response = client.item.get_item_detail(
#         update_time_from = dt_to_timestamp(dt_from),
#         update_time_to = dt_to_timestamp(dt_to),
#         item_id = item_id,
#     )
#     return response


def GetItemDetail(item_id):
    response = client.item.get_item_detail(
        item_id = item_id
    )
    return response


def main():

    ################################################################################
    # Shopee GetItemsList
    ################################################################################

    if dt_from is not None and dt_to is not None:
        start_dt = parse_dt(dt_from, country)
        end_dt = parse_dt(dt_to, country)
        day_range = 14

        date_list = []

        daypairs = grouper(daterange(start_dt, end_dt), day_range)

        for each in list(daypairs):

            start_dt = datetime.combine(each[0], time.min) if each[0] is not None else each[0]
            end_dt = datetime.combine(each[-1], time.max) if each[-1] is not None else datetime.combine(each[0] + timedelta(days=day_range), time.max)

            date_list.append(
                {
                    "start": dt_to_timestamp(start_dt),
                    "end": dt_to_timestamp(end_dt)
                }
            )

    else:
        print("Please insert dt_from and dt_to parameter")


    # for each in date_list:
    #     print(timestamp_to_dt(each["start"]), timestamp_to_dt(each["end"]))

    for date in date_list:

        start_dt = date["start"]
        end_dt = date["end"]

        # if data is more than one page, the offset can be some entry to start next call.
        offset = 0
        # Maximum number of entries (<= 100) to retrieve per page
        limit = 50

        # Get Items Total value from Shopee GetItemsList API
        response = GetItemList(
            update_time_from = start_dt, 
            update_time_to = end_dt, 
            offset = 0, 
            limit = 1
        )

        data = json.loads(response)

        if data["status"] == 200:
            total = data["total"]
            print("*" * 100)
            print(f"Working at Offset:{offset} From:{str(timestamp_to_dt(start_dt))} To:{str(timestamp_to_dt(end_dt))} Total:{total}")

        else:
            total = None
            print(data)


        # If total have value
        if total:

            # Looping over the GetItemsList API until more = false
            # Loop when the offset is equal or less than the total value
            while offset <= total:

                # Get Shopee GetItemsList API data
                response = GetItemList(
                    update_time_from = start_dt, 
                    update_time_to = end_dt, 
                    offset = offset, 
                    limit = limit
                )

                data = json.loads(response)

                if data["status"] == 200:
                    
                    # List use to call Item detail later
                    item_id_list = []

                    # Add responsed items to item temp list
                    for item in data["items"]:

                        # Item list data store here
                        item_list.append(item)

                        # Copy the item id into this list
                        item_id_list.append(item["item_id"])

                    
                    #################################################
                    # Call Shopee item_detail_list API start
                    #################################################

                    # # Retrieve item_id, get their item detail one by one
                    # for i in range(len(item_id_list)):  
                    #     #item_detail_list.append(GetItemDetail(update_time_from , update_time_to , item_id_list[i]))
                    #     item_detail_list.append(GetItemDetail(1614556800000, 1617321599000, item_id_list[i]))

                    # Retrieve item_id, get their item detail one by one
                    for item_id in item_id_list:
                        response = GetItemDetail(item_id)
                        item_detail_list.append(response["item"])
                        print(f"{item_id} {response['item']['name']}")
                    
                               
                    #################################################
                    # Call Shopee item_detail_list API end
                    #################################################

                    # Incremental offset if true
                    if data["more"] == True:
                        offset = offset + limit

                    else:
                        # Stop the looping
                        # Completed and no more offset
                        print(
                            json.dumps(
                                {
                                    "status": 200,
                                    "msg": "Done", 
                                    "update_time_from": str(timestamp_to_dt(data["update_time_from"])), 
                                    "update_time_to": str(timestamp_to_dt(data["update_time_to"])),
                                    "total": int(data["total"])
                                }
                            )
                        )

                        break
                        
                else:
                    print(data)
                    # Stop the looping
                    break

        
    # Generate Item List .csv file
    if item_list:
        item_list_data = pd.DataFrame(item_list)
        item_list_data.to_csv("item_list.csv", index=False)
        print(item_list_data)

                # Store Item list in MongoDB
        with MongoClient("mongodb://127.0.0.1:27017/?") as client:
            db = client["shopee"]
            for item in item_list:
                db.ItemList.update_many(
                    {
                        "item_id": { "$eq": item["item_id"] }
                    },
                    {
                        "$set": {
                            "item_id": item["item_id"],
                            "shopid": item["shopid"],
                            "update_time": item["update_time"],	
                            "status": item["status"],
                            "item_sku": item["item_sku"],
                            "variations": item["variations"],
                            "is_2tier_item": item["is_2tier_item"],
                            "tenures": item["tenures"],
                        }
                    }, upsert=True
                )
        mongoexport --uri="mongodb://mongodb0.example.com:27017/reporting"  --collection=events  --out=events.json [additional options]

    # if item_detail_list:
    #     bn = pd.DataFrame(item_detail_list)['item']
    #     item_list_data2 = pd.DataFrame.from_records(bn).head(1000)
    #     item_list_data2.to_csv("item_detail_list.csv")
    #     print(item_list_data2)

    # Generate Item Detail List .csv file
    if item_detail_list:
        item_detail_list_data = pd.DataFrame(item_detail_list)
        item_detail_list_data.to_csv("item_detail_list.csv", index=False)

        # Store Item details in MongoDB
        with MongoClient("mongodb://127.0.0.1:27017/?") as client:
            db = client["shopee"]
            for row in item_detail_list:
                db.ItemDetailList.update_many(
                    {
                        "item_id": { "$eq": row["item_id"] }
                    },
                    {
                        "$set": {
                               "status": row["status"],	
                               "original_price": row["original_price"],
                               "update_time": row["update_time"],	
                               "package_width": row["package_width"],	
                               "description": row["description"],	
                               "weight": row["weight"],	
                               "views": row["views"],
                               "rating_star": row["rating_star"],	
                               "price": row["price"],
                               "shopid": row["shopid"],	
                               "sales": row["sales"],
                               "discount_id": row["discount_id"],
                               "images": row["images"],
                               "create_time": row["create_time"],	
                               "likes": row["likes"],
                               "wholesales": row["wholesales"],
                               "item_id": row["item_id"],
                               "logistics": row["logistics"],
                               "tenures": row["tenures"],	
                               "condition": row["condition"],
                               "cmt_count": row["cmt_count"],
                               "package_height": row["package_height"],	
                               "days_to_ship": row["days_to_ship"],
                               "name": row["name"],
                               "currency": row["currency"],
                               "item_dangerous": row["item_dangerous"],	
                               "item_sku": row["item_sku"],
                               "variations": row["variations"],	
                               "is_2tier_item": row["is_2tier_item"],	
                               "size_chart": row["size_chart"],	
                               "package_length": row["package_length"],	
                               "video_info": row["video_info"],
                               "is_pre_order": row["is_pre_order"],	
                               "has_variation": row["has_variation"],	
                               "attributes": row["attributes"],
                               "category_id": row["category_id"],	
                               "reserved_stock": row["reserved_stock"],	
                               "stock": row["stock"],
                        }
                    },upsert=True
                )

    
if __name__ == "__main__":

    #You can get the all three id by register shopee
    shopid = #ID
    partner_id = #ID
    secret_key = #ID

    client = pyshopee.Client(shop_id=shopid, partner_id=partner_id, secret_key=secret_key)
    #You need to have mongoDB for this process
    client = pymongo.MongoClient("mongodb+srv://(NAME:PASSWORD)@cluster0.ziwwn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
   
   #Check Server availablity
    client1 = MongoClient()
    if client1.admin.command('ismaster'):
        print("Server available")
    elif ConnectionFailure:
        print("Server not available")

    
    dt_from = "2021-04-01 00:00:00" 
    dt_to = "2021-04-15 23:59:59"

    country = "MY"

    # Temp list for Shopee GetItemsList
    item_list = []

    # Temp list for Shopee GetItemDetail
    item_detail_list = []

    # run
    print(main())
