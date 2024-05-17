from airflow.decorators import dag,task
import requests
import pandas as pd
import io
from string import capwords
import pandas_gbq
from google.oauth2 import service_account

gbq_credentials = service_account.Credentials.from_service_account_file('/Users/abhijeetkarmakar/projects/data engineering/Uber Restaurant Menu/service account key.json')

@dag("restaurant_menu",schedule=None,tags=['restaurant menu'])
def restaurant_menu_processing():
    @task()
    def fetch_restaurant_menu_data():
        url = 'https://storage.googleapis.com/restaurant-menu-raw-data/restaurant-menus.csv'
        response = requests.get(url)
        return pd.read_csv(io.StringIO(response.text), sep=',')
    
    @task(multiple_outputs=True)
    def transform_restaurant_menu_data(data:pd.DataFrame):
        #Determining menu category dimension table
        df_restaurant_menu = data.copy()
        del data

        menu_category_dim = pd.DataFrame(df_restaurant_menu['category'].unique(),columns=['category_name'])
        menu_category_dim['category_name'] = menu_category_dim.apply(lambda row: row.category_name.replace('&amp;', '&'),axis=1)
        menu_category_dim['category_name'] = menu_category_dim.apply(lambda row: row.category_name.replace('\n;', ''),axis=1)
        menu_category_dim['category_name'] = menu_category_dim.apply(lambda row: capwords(row.category_name),axis=1)
        menu_category_dim = menu_category_dim[~menu_category_dim.duplicated(subset='category_name',keep=False)]
        menu_category_dim = menu_category_dim.reset_index(drop=True)
        menu_category_dim['category_id'] = menu_category_dim.index

        #Determining pre-processed restaurant menu fact table
        restaurant_fact_table = df_restaurant_menu.copy()
        restaurant_fact_table.drop(columns=['name','description'],inplace=True)
        restaurant_fact_table['price'] = restaurant_fact_table.apply(lambda row: row.price.split()[0],axis=1)
        restaurant_fact_table['category'] = restaurant_fact_table.apply(lambda row: row.category.replace('&amp;', '&'),axis=1)
        restaurant_fact_table['category'] = restaurant_fact_table.apply(lambda row: row.category.replace('\n;', ''),axis=1)
        restaurant_menu_fact = pd.merge(left=restaurant_fact_table,right=menu_category_dim,left_on='category',right_on='category_name',how='inner')
        restaurant_menu_fact = restaurant_menu_fact[['restaurant_id','category_id','price']]

        return {"menu_category_dim" : menu_category_dim.to_dict(orient="dict"),"restaurant_menu_fact" : restaurant_menu_fact.to_dict(orient="dict")}

    @task()
    def fetch_restaurant_data():
        url = 'https://storage.googleapis.com/restaurant-menu-raw-data/restaurants.csv'
        response = requests.get(url)
        return pd.read_csv(io.StringIO(response.text), sep=',')
    
    @task(multiple_outputs=True)
    def transform_restaurant_data(data:pd.DataFrame):
        df_restaurants = data.copy()
        del data

        #Building price range dimension model
        price_range_dim = pd.DataFrame(df_restaurants['price_range'].unique(),columns=['price_range'])
        price_range_dim['price_range_name'] = price_range_dim.apply(lambda row: 'Inexpensive' if row.price_range == '$' else 'Moderately expensive' if row.price_range == '$$' else 'Expensive' if row.price_range == '$$$' else 'Unknown' if pd.isna(row.price_range) else 'Very expensive',axis=1)
        price_range_dim['price_range_id'] = price_range_dim.index
        price_range_dim = price_range_dim.drop(columns=['price_range'])

        #Building zip code dimension model
        zip_code_dim = pd.DataFrame(df_restaurants.zip_code.unique(),columns=['zip_code'])
        zip_code_dim['zip_code_id'] = zip_code_dim.index

        #Building restaurant category dimension model
        restaurant_category_dim = pd.DataFrame(df_restaurants.category.unique(),columns=['category_name'])
        restaurant_category_dim = restaurant_category_dim.apply(lambda row: row.str.split(',').explode())
        restaurant_category_dim = restaurant_category_dim.drop_duplicates(subset=['category_name'],keep='first')
        restaurant_category_dim = restaurant_category_dim.reset_index(drop=True)
        restaurant_category_dim['category_id'] = restaurant_category_dim.index

        #Building scores dimension model
        scores_dim = pd.DataFrame(df_restaurants.score.unique(),columns=['score'])
        scores_dim['score'] = scores_dim.apply(lambda row: 0 if pd.isna(row.score) == True else row.score,axis=1)
        scores_dim = scores_dim.sort_values(by='score')
        scores_dim.reset_index(inplace=True,drop=True)
        scores_dim['score_id'] = scores_dim.index

        #Building restaurant dimension model
        restaurant_dim = df_restaurants.copy()
        del df_restaurants
        restaurant_dim['score'] = restaurant_dim.apply(lambda row: 0 if pd.isna(row.score) else row.score,axis=1)
        restaurant_dim['ratings'] = restaurant_dim.apply(lambda row: 0 if pd.isna(row.ratings) else row.ratings,axis=1)
        restaurant_dim['price_range'] = restaurant_dim.apply(lambda row: 'Inexpensive' if row.price_range == '$' else 'Moderately expensive' if row.price_range == '$$' else 'Expensive' if row.price_range == '$$$' else 'Unknown' if pd.isna(row.price_range) else 'Very expensive',axis=1)
        restaurant_dim = restaurant_dim.set_index(['id','position','name','score','ratings','price_range','full_address','zip_code','lat','lng']).apply(lambda row: row.str.split(',').explode()).reset_index()
        restaurant_dim = restaurant_dim.merge(right=restaurant_category_dim,left_on='category',right_on='category_name').merge(right=scores_dim,left_on='score',right_on='score').merge(right=price_range_dim,left_on='price_range',right_on='price_range_name').merge(right=zip_code_dim,left_on='zip_code',right_on='zip_code')
        restaurant_dim.rename(inplace=True,columns={'position':'search_position'})
        restaurant_dim = restaurant_dim[['id','category_id','price_range_id','score_id','zip_code_id','search_position','name','full_address','ratings','lat','lng']]

        return {"price_range_dim" : price_range_dim.to_dict(orient="dict"), 
        "zip_code_dim" : zip_code_dim.to_dict(orient="dict"),
        "restaurant_category_dim" : restaurant_category_dim.to_dict(orient="dict"),
        "scores_dim" : scores_dim.to_dict(orient="dict"),
        "restaurant_dim" : restaurant_dim.to_dict(orient="dict")}
    
    @task()
    def cumulative_transformer_exporter(restaurant_data, restaurant_menu_data):
        restaurant_menu_fact = pd.DataFrame(restaurant_menu_data['restaurant_menu_fact'])
        restaurant_dim = pd.DataFrame(restaurant_data['restaurant_dim'])
        df_restaurant_menu_fact = restaurant_menu_fact.merge(restaurant_dim,left_on='restaurant_id',right_on='id',suffixes=('__menu','__restaurant'))
        to_store = {"menu_category_dim": restaurant_menu_data['menu_category_dim'],"restaurant_menu_fact" : df_restaurant_menu_fact.to_dict(orient="dict")}
        for table_name,table in to_store.items():
            pandas_gbq.to_gbq(pd.DataFrame(table),destination_table="uber_restaurant_data_engineering.{}".format(table_name),project_id="psyched-zone-422610-j4",if_exists="replace",credentials=gbq_credentials)
        

       
    @task()
    def data_exporter(data:dict):
        for table_name,table in data.items():
            pandas_gbq.to_gbq(pd.DataFrame(table),destination_table="uber_restaurant_data_engineering.{}".format(table_name),project_id="psyched-zone-422610-j4",if_exists="replace",credentials=gbq_credentials)
        

    df_restaurant_menu = fetch_restaurant_menu_data()
    menu_data = transform_restaurant_menu_data(df_restaurant_menu)
    df_restaurant_data = fetch_restaurant_data()
    restaurant_data = transform_restaurant_data(df_restaurant_data)
    cumulative_transformer_exporter(restaurant_data,menu_data)
    data_exporter(restaurant_data)

restaurant_menu_processing()
