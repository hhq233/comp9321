import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re

studentid = os.path.basename(sys.modules[__name__].__file__)


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


def question_1(city_pairs):
    """
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    csv_file = "city_pairs.csv"
    df1 = pd.read_csv(csv_file)
    # if In < Out -> OUT, elif In > Out -> In, else SAME
    df1["passenger_in_out"] = df1.apply(lambda row: "OUT" if row["Passengers_In"] < row["Passengers_Out"] else ("IN" if row["Passengers_In"] > row["Passengers_Out"] else "SAME"), axis = 1)
    df1["freight_in_out"] = df1.apply(lambda row: "OUT" if row["Freight_In_(tonnes)"] < row["Freight_Out_(tonnes)"] else ("IN" if row["Freight_In_(tonnes)"] > row["Freight_Out_(tonnes)"] else "SAME"), axis=1)
    df1["mail_in_out"] = df1.apply(lambda row: "OUT" if row["Mail_In_(tonnes)"] < row["Mail_Out_(tonnes)"] else ("IN" if row["Mail_In_(tonnes)"] > row["Mail_Out_(tonnes)"] else "SAME"), axis=1)
    #################################################
    log("QUESTION 1", output_df=df1[["AustralianPort", "ForeignPort", "passenger_in_out", "freight_in_out", "mail_in_out"]], other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: dataframe df2
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...


    df = {
        "PassengerInCount" : df1[df1["passenger_in_out"] == 'IN'].groupby("AustralianPort")["passenger_in_out"].count(),
        "PassengerOutCount": df1[df1["passenger_in_out"] == 'OUT'].groupby("AustralianPort")["passenger_in_out"].count(),
        "FreightInCount": df1[df1["freight_in_out"] == 'IN'].groupby("AustralianPort")["freight_in_out"].count(),
        "FreightOutCount": df1[df1["freight_in_out"] == 'OUT'].groupby("AustralianPort")["freight_in_out"].count(),
        "MailInCount" : df1[df1["mail_in_out"] == 'IN'].groupby("AustralianPort")["mail_in_out"].count(),
        "MailOutCount": df1[df1["mail_in_out"] == 'OUT'].groupby("AustralianPort")["mail_in_out"].count()
    }
    df2 = pd.DataFrame(df).fillna(0).astype(int).sort_values(by = "PassengerInCount", ascending = False)
    df2.reset_index(inplace=True)
    #################################################
    log("QUESTION 2", output_df=df2, other=df2.shape)
    return df2


def question_3(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    # Your code goes here ...
    num_of_Month = df1["Month"].nunique()

    # group by Country and sum them and divide total unique number of month
    df = {
        "Passengers_in_average" : df1.groupby("Country")["Passengers_In"].sum().apply(lambda x:x/num_of_Month),
        "Passengers_out_average": df1.groupby("Country")["Passengers_Out"].sum().apply(lambda x:x/num_of_Month),
        "Freight_in_average": df1.groupby("Country")["Freight_In_(tonnes)"].sum().apply(lambda x:x/num_of_Month),
        "Freight_out_average": df1.groupby("Country")["Freight_Out_(tonnes)"].sum().apply(lambda x:x/num_of_Month),
        "Mail_in_average": df1.groupby("Country")["Mail_In_(tonnes)"].sum().apply(lambda x: x/num_of_Month),
        "Mail_out_average": df1.groupby("Country")["Mail_Out_(tonnes)"].sum().apply(lambda x: x/num_of_Month),
    }
    # sort by Passengers_in_average
    df3 = pd.DataFrame(df).sort_values(by = "Passengers_in_average")
    # including 2 dp
    df3 = df3.round(2)
    df3.reset_index(inplace=True)
    #################################################
    log("QUESTION 3", output_df=df3, other=df3.shape)
    return df3


def question_4(df1):
    """
    :param df1: the dataframe created in question 3
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df = df1[df1["Passengers_Out"] > 0]
    # select country and month from df and drop duplicates
    new_df = df[["AustralianPort", "Month", "Country"]].drop_duplicates()
    df4 = new_df.groupby("Country").count().drop(["Month"], axis = 1)
    df4.reset_index(inplace=True)
    # rename AustralianPort column
    df4.rename(columns = {"AustralianPort" : "Unique_ForeignPort_Count"}, inplace = True)
    # sort df4
    df4.sort_values(by = ["Unique_ForeignPort_Count", "Country"], ascending = [False, True],ignore_index=True,inplace=True)
    df4 = df4.head(5)
    #################################################

    log("QUESTION 4", output_df=df4, other=df4.shape)
    return df4


def question_5(seats):
    """
    :param seats : the path to dataset
    :return: df5
            Data Type: dataframe
            Please read the assignment specs to know how to create the  output dataframe
    """
    #################################################
    # Your code goes here ...
    csv_file = "seats.csv"
    df5 = pd.read_csv(csv_file)
    df5["Source_City"] = df5.apply(lambda row: row["Australian_City"] if row["In_Out"] == "O" else row["International_City"], axis = 1)
    df5["Destination_City"] = df5.apply(lambda row: row["International_City"] if row["In_Out"] == "O" else row["Australian_City"], axis=1)
    #################################################
    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5


def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
    """
    """
    This dataframe selects ["Airline", "Source_City", "Destination_City", "Year", "Route"] from df5,
    and grouped by ["Airline", "Source_City", "Destination_City", "Year"] and count the route, 
    so that we can find the route of each airline's service in recent years. Use pivot convert "Years" to columns, 
    so that we can easily find a change tendency of each airline's service in different route by years.
    By use 'select' function, we can observe that for a same route, if many airlines have declined trend on this
    route's service, may mean that this route is not competition, but also maybe this route is not liked by customer.
    In addition, we can also observe that each airline's service distribution and its change by year.
    """
    #################################################
    # Your code goes here ...
    subdf = df5[["Airline", "Source_City", "Destination_City", "Year", "Route"]].groupby(["Source_City", "Destination_City", "Airline", "Year"]).count()
    df6 = subdf.reset_index().pivot(["Airline", "Source_City", "Destination_City"],"Year", "Route").fillna(0).astype(int)
    # convert columns type from int to str
    df6.columns = df6.columns.astype(str)
    #################################################
    log("QUESTION 6", output_df=df6, other=df6.shape)
    return df6


def question_7(seats, city_pairs):
    """
    :param seats: the path to dataset
    :param city_pairs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """
    """
    To show tendency over time for seat utilisation around the world, line chart is a good choice.
    Therefore my charts' x axis represents times, y axis represents seat utilisation. Because there are too many months
    from Sep-03 to Sep-22 which makes figure too messy, I choose year as unit.
    To get yearly seat utilisation, I firstly merge two csv with same airport, month, year, to get monthly passengers'
    in, out and airlines' seat utilisation. Calculate seat utilisation with total max_seat in market is meaningless for 
    a airline, so I choose to use mean value of airlines max seat to get seat utilisation in In and Out respectively. 
    Then I use the mean value of monthly seat utilisation to represent yearly seat utilisation in the final result.
    In the output picture, each region's in and out seat utilisation's change with time is plotted in one figure, which 
    can show each region's change of seat utilisation with time clearly.  
    """
    #################################################
    # Your code goes here ...
    #################################################
    csv_file = "city_pairs.csv"
    df1 = pd.read_csv(csv_file)
    csv_file = "seats.csv"
    df2 = pd.read_csv(csv_file)
    # merge data with same australianport, foreighport, year, month, and country
    merge_df = pd.merge(left = df1,
                        right = df2,
                        left_on = ["Year", "Month", "AustralianPort", "ForeignPort", "Country"],
                        right_on = ["Year", "Month", "Australian_City", "International_City", "Port_Country"]
                        )
    # select "Year", "Month", "AustralianPort", "ForeignPort", "Port_Region", "Passengers_In", "Passengers_Out",
    # "Max_Seats", "In_Out" from original merge_df
    sub_df = merge_df[["Year", "Month", "AustralianPort", "ForeignPort", "Port_Region", "Passengers_In", "Passengers_Out", "Max_Seats", "In_Out"]]
    # get the mean max_seats of airlines in each month
    sub_df = sub_df.groupby(["Year", "Month", "AustralianPort", "ForeignPort", "Port_Region", "Passengers_In", "Passengers_Out", "In_Out"], sort = False)["Max_Seats"].mean()
    new_df = sub_df.reset_index()
    # calculate in and out seat_utilisation respectively
    new_df["Seat_Utilisation"] = new_df.apply(lambda row: row["Passengers_Out"]/row["Max_Seats"] if row["In_Out"] == "O" else row["Passengers_In"]/row["Max_Seats"], axis = 1)
    # get the mean seat_utilisation for a month
    df7 = new_df.groupby(["Year", "Month", "Port_Region", "In_Out"], sort = False)["Seat_Utilisation"].mean()
    df7 = df7.reset_index()
    # get the mean seat_utilisation for a year
    df7 = df7.groupby(["Year", "Port_Region", "In_Out"], sort = False)["Seat_Utilisation"].mean()
    df7 = df7.reset_index()

    # prepare to out a picture where each region are show in a line figure
    Region_List = df7["Port_Region"].unique()
    fig, axes = plt.subplots(nrows=int(len(Region_List)/2), ncols=2)
    fig.suptitle("Region Seat Utilisation's Trend With Year", fontsize=16)
    num_Region = 0
    for region in Region_List:
        # plot an In line and an Out line in a figure
        region_in_df = df7.query("Port_Region == @region and In_Out == 'I'")
        region_out_df = df7.query("Port_Region == @region and In_Out == 'O'")
        #axes[num_Region//2, num_Region%2].set_title('subplot 1')
        ax = region_in_df.plot.line(x='Year', y='Seat_Utilisation', label=region + '_' + 'In', figsize=(10, 10), ax = axes[num_Region//2, num_Region%2])
        ax = region_out_df.plot.line(x='Year', y='Seat_Utilisation', label=region + '_' + 'Out', figsize=(10, 10), ax = ax)
        num_Region+=1
        ax.set_xticks(range(2003, 2025, 3))

    plt.savefig("{}-Q7.png".format("z5295162"))


if __name__ == "__main__":
    #df1 = question_1("city_pairs.csv")
    #df2 = question_2(df1.copy(True))
    #df3 = question_3(df1.copy(True))
    #df4 = question_4(df1.copy(True))
    #df5 = question_5("seats.csv")
    #f6 = question_6(df5.copy(True))
    question_7("seats.csv", "city_pairs.csv")