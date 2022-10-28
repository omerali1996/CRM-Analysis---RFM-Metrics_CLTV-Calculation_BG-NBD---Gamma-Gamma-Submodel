import pandas as pd
import datetime as dt
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

#Görev1

#Adım1
data=pd.read_csv("FLO_RFM_Analizi/flo_data_20k.csv")
df=data.copy()

#Adım2
def ourlier_threshold(df,col):
    Q3 = df[col].quantile(0.99)
    Q1 = df[col].quantile(0.01)
    quantiles_range = Q3 - Q1
    low_limit = round((Q1 - 1.5 * quantiles_range))
    up_limit = round((Q3 + 1.5 * quantiles_range))
    return low_limit,up_limit

def replace_thresholds(df,col):
    low_limit, up_limit = ourlier_threshold(df,col)
    df.loc[df[col] > up_limit,col] = up_limit

    df.loc[df[col] < low_limit, col] = low_limit

#Adım3


df.info()

df.describe()
df.isnull().sum()
[replace_thresholds(df,variable) for variable in
 df.loc[:,df.columns.str.contains("total_ever")].columns]

df.loc[:,df.columns.str.contains("total_ever")]=df.loc[:,df.columns.str.
                                                         contains("total_ever")].astype(int)


#Adım4

df["Omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

df["Total_Price"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

df.describe()
#Adım5

df.loc[:,df.columns.str.contains("date")]=df.loc[:,df.columns.str.contains("date")]\
    .apply(lambda x :pd.to_datetime(x))

df.info()

#Görev2
# Adım1

df["last_order_date"].max()

today=dt.datetime(2021,6,2)

#Adım2
df.info()

df["last_first"]=(df["last_order_date"]-df["first_order_date"]).dt.days

cltv_df=df.groupby("master_id").agg({"last_first":lambda recency: recency,
                                     "first_order_date":lambda T: (today -T.min()).days,
                             "Omnichannel": lambda frequency: sum(frequency),
                             "Total_Price": lambda monetary: sum(monetary)})



cltv_df.columns=["recency","T","frequency","monetary"]

cltv_df.describe().T
cltv_df["recency_cltv_weekly"]= cltv_df["recency"]/7

cltv_df["T_weekly"]= cltv_df["T"]/7

cltv_df["monetary_cltv_avg"]= cltv_df["monetary"]/cltv_df["frequency"]

#Görev3

#Adım1

bgf=BetaGeoFitter(0.001)
bgf.fit(cltv_df["frequency"],cltv_df["recency_cltv_weekly"],cltv_df["T_weekly"])

cltv_df["exp_sales_3_months"]=bgf.conditional_expected_number_of_purchases_up_to_time(4*3,
                                                        cltv_df["frequency"],
                                                        cltv_df["recency_cltv_weekly"]
                                                        ,cltv_df["T_weekly"])

cltv_df["exp_sales_6_months"]=bgf.conditional_expected_number_of_purchases_up_to_time(4*6,
                                                        cltv_df["frequency"],
                                                        cltv_df["recency_cltv_weekly"]
                                                        ,cltv_df["T_weekly"])


#Adım2

ggf=GammaGammaFitter(0.01)
ggf.fit(cltv_df["frequency"],cltv_df["monetary_cltv_avg"])

cltv_df["exp_average_value"] = ggf.conditional_expected_average_profit(cltv_df["frequency"],
                                                                       cltv_df["monetary_cltv_avg"])

#Adım3

cltv_df["cltv"]=ggf.customer_lifetime_value(bgf,
                                            cltv_df["frequency"],
                                            cltv_df["recency_cltv_weekly"],
                                            cltv_df["T_weekly"],
                                            cltv_df["monetary_cltv_avg"],
                                            time=6,
                                            freq="W")

cltv_df.sort_values(by="cltv",ascending=False).head(20)




#Görev4

#Adım1
cltv_df.describe().T

cltv_df["segment"]=pd.qcut(cltv_df["cltv"],4,labels=["D","C","B","A"])


#Adım2
cltv_df.groupby("segment").agg({"mean","sum"})

cltv_df.reset_index()

cltv_df[cltv_df["segment"]=="C"][["exp_sales_3_months","master_id"]]
