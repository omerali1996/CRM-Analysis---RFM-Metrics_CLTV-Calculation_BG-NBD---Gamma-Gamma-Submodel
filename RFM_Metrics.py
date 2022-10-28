import pandas as pd
import datetime as dt

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

#Görev1
#Adım1
data=pd.read_csv("FLO_RFM_Analizi/flo_data_20k.csv")
df=data.copy()

#Adım2
df.head(10)
df.columns
df.describe().T
df.isnull().sum()
df.info()

#Adım3


df["Total_Price"]= df["customer_value_total_ever_offline"] +\
                   df["customer_value_total_ever_online"]

df["Omnichannel"]= df["order_num_total_ever_offline"] +\
                   df["order_num_total_ever_online"]

#Adım4

df.loc[:,df.columns.str.contains("date")]=\
    df.loc[:,df.columns.str.contains("date")]\
        .apply(lambda x:pd.to_datetime(x))

#Adım5
df.groupby("master_id").agg({"Omnichannel":"sum",
                             "Total_Price":"sum"})

#Adım6
df.sort_values(by="Total_Price",ascending=False)["master_id"].head(10)

#Adım7
df.sort_values(by="Omnichannel",ascending=False)["master_id"].head(10)

#Adım8
def create_data(df):

    data = pd.read_csv("FLO_RFM_Analizi/flo_data_20k.csv")
    df = data.copy()

    df.head(10)
    df.columns
    df.describe().T
    df.isnull().sum()
    df.info()

    df["Total_Price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

    df["Omnichannel"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]

    df.loc[:, df.columns.str.contains("date")] = \
        df.loc[:, df.columns.str.contains("date")]\
            .apply(lambda x: pd.to_datetime(x))

    df.groupby("master_id").agg({"Omnichannel": "sum",
                                 "Total_Price": "sum"})

    df.sort_values(by="Total_Price", ascending=False)["master_id"].head(10)

    df.sort_values(by="Omnichannel", ascending=False)["master_id"].head(10)
    return df

dataframe=create_data(df)

########
#Görev2

df["last_order_date"].max()

today=dt.datetime(2021,6,1)

df.info()

rfm=df.groupby("master_id").agg({"last_order_date":lambda x : (today-x.max()).days,
                             "Omnichannel":lambda x:sum(x),
                             "Total_Price":lambda x:sum(x)})

rfm.columns=["recency","frequency","monetary"]
#Görev3

rfm["rec_score"]=pd.qcut(rfm["recency"],5,labels=[5,4,3,2,1])

rfm["mon_score"]=pd.qcut(rfm["monetary"],5,labels=[1,2,3,4,5])

rfm["freq_score"]=pd.qcut(rfm["frequency"].rank(method="first"),5,labels=[1,2,3,4,5])

rfm["rfm_score"]=rfm["rec_score"].astype(str)+rfm["freq_score"].astype(str)

#Görev4
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"]=rfm["rfm_score"].replace(seg_map,regex=True)

rfm[["recency","frequency","monetary","segment"]].\
    groupby("segment").agg(["mean"])
#Görev5

#adım1

rfm.reset_index(inplace=True)

df=df.merge(rfm,on="master_id",how="left")

new_df=pd.DataFrame()
new_df=(df[(df["interested_in_categories_12"].str.contains("KADIN"))
   & ((df["segment"]=="champions")
      | (df["segment"]=="loyal_customers"))]["master_id"])


new_df.to_csv("Case1_Görev5_Adım1.csv")
#adım2
new2_df=pd.DataFrame()


new2_df=(df[(df["segment"]=="cant_loose")
   | ((df["segment"]=="about_to_sleep")
      | (df["segment"]=="new_customers"))]["master_id"])

new2_df.to_csv("Case1_Görev5_Adım2.csv")
