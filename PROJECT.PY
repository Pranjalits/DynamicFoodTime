import pandas as pd
import numpy as np
import requests
import mysql.connector
from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor 
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import OneHotEncoder



api_key_ow = '499f1171108e6c7bf613c0537f7dcf7c'           # open weather api key
api_key_tt = "YKPHuZ74pdng8wmhRnlxECWA6r2DQu36"           # tom tom api key

API_KEY = '5b3ce3597851110001cf62483b7f8ce3024e411daaea548768eb8f03'
API_URL = 'https://api.openrouteservice.org/v2/directions/driving-car'
    
df = pd.read_csv('temp_csv.csv')



conn = mysql.connector.connect(
    host="localhost",        
    user="root",     
    password="your_password", 
    database="ml_project" 
)


cursor = conn.cursor()



def EDA():
    from ydata_profiling import ProfileReport
    prof = ProfileReport(df, correlations={"pearson": {"calculate": True}, "spearman": {"calculate": True}})
    prof.to_file(output_file="output_Final.html")



def Handling_missing_values():
    
    numeric_columns = ['Delivery_person_Age', 'Delivery_person_Ratings','temperature','humidity','precipitation','Distance (km)']
    categorical_columns = ['Type_of_order', 'Type_of_vehicle','weather_description','Traffic_labels']
    df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
    for col in categorical_columns:
        df[col].fillna(df[col].mode()[0], inplace=True)

    df.to_csv('Filled_missing_value.csv', index=False)

    print("Missing values have been filled for specified columns and saved in the same file.")



def Ordinal_Encoding():
    df=pd.read_csv('Filled_missing_value.csv')
    column_to_encode = 'Traffic_labels'
    category_order = [['very low', 'low', 'moderate','high']]
    encoder = OrdinalEncoder(categories=category_order)
    df[[column_to_encode]] = encoder.fit_transform(df[[column_to_encode]])
    df.to_csv('Ordinal_encoded.csv', index=False)
    print("Ordinal encoding applied and saved to 'your_file.csv'")






def Xgboost_without_pca():
    # Load the dataset and select specific columns
    file_path = 'Ordinal_encoded.csv'  # Replace with your actual file path
    selected_columns = ['Delivery_person_Age', 'Delivery_person_Ratings','Type_of_order','Type_of_vehicle','Distance (km)',
                         'temperature', 'precipitation', 'humidity', 'weather_description','Traffic_labels', 'Total_time']
    data = pd.read_csv(file_path, usecols=selected_columns)

    # Clean the target column by converting non-numeric values to NaN and dropping them
    data['Total_time'] = pd.to_numeric(data['Total_time'], errors='coerce')
    data = data.dropna(subset=['Total_time'])

    # Convert categorical features to category dtype (or use label encoding if you prefer)
    categorical_columns = ['Type_of_order', 'Type_of_vehicle', 'weather_description']
    
    for col in categorical_columns:
        data[col] = data[col].astype('category')

    # Define feature columns and target column
    target_column = 'Total_time'
    X = data.drop(columns=[target_column])
    y = data[target_column].astype(float)

    # Optionally, Label encode categorical variables (if necessary)
    # label_encoder = LabelEncoder()
    # for col in categorical_columns:
    #     X[col] = label_encoder.fit_transform(X[col])

    # Split dataset into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

    # Initialize the XGBoost model for regression
    model = XGBRegressor(n_estimators=100, random_state=42, enable_categorical=True)

    # Train the model
    model.fit(X_train, y_train)  # Fit the model to the training data

    # Now you can make predictions
    y_pred = model.predict(X_test)

    # Evaluate the model
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    # Display Model Performance Metrics Table
    metrics_df = pd.DataFrame({
        'Metric': ['Mean Absolute Error (MAE)', 'Mean Squared Error (MSE)', 'Root Mean Squared Error (RMSE)', 'R-squared (R²)'],
        'Value': [mae, mse, rmse, r2]
    })
    print(metrics_df)



    # Actual vs. Predicted Plot
    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, y_pred, color='purple', alpha=0.6)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', linewidth=2)  # Diagonal line
    plt.xlabel('Actual Travel Time')
    plt.ylabel('Predicted Travel Time')
    plt.title('Actual vs. Predicted Travel Time')
    plt.show()

    # Residual Plot
    residuals = y_test - y_pred
    plt.figure(figsize=(8, 6))
    plt.scatter(y_pred, residuals, color='blue', alpha=0.6)
    plt.axhline(y=0, color='r', linestyle='--', linewidth=2)
    plt.xlabel('Predicted Travel Time')
    plt.ylabel('Residuals')
    plt.title('Residual Plot')
    plt.show()

    # Error Distribution Plot
    plt.figure(figsize=(8, 6))
    sns.histplot(residuals, kde=True, color='orange')
    plt.xlabel('Prediction Error')
    plt.title('Error Distribution')
    plt.show()

    # Predicted vs Actual Delivery Time Distribution
    plt.figure(figsize=(8, 6))
    sns.histplot(y_test, color='blue', label='Actual', kde=True, alpha=0.5)
    sns.histplot(y_pred, color='red', label='Predicted', kde=True, alpha=0.5)
    plt.xlabel('Delivery Time (Minutes)')
    plt.title('Predicted vs Actual Delivery Time Distribution')
    plt.legend()
    plt.show()



    # Return the trained model to be used later
    return model




def Xgboost_with_pca():
    # Load the dataset and select specific columns
    file_path = 'Ordinal_encoded.csv'  # Replace with your actual file path
    selected_columns = ['Delivery_person_Age', 'Delivery_person_Ratings', 'Type_of_order', 'Type_of_vehicle', 
                         'Distance (km)', 'temperature', 'precipitation', 'humidity', 'weather_description', 'Traffic_labels', 'Total_time']
    data = pd.read_csv(file_path, usecols=selected_columns)

    # Display first few rows to understand the structure
    print(data.head())

    # Clean the target column by converting non-numeric values to NaN and dropping them
    data['Total_time'] = pd.to_numeric(data['Total_time'], errors='coerce')
    data = data.dropna(subset=['Total_time'])

    # Convert categorical columns to numerical values using LabelEncoder
    categorical_columns = ['Type_of_order', 'Type_of_vehicle', 'weather_description']
    label_encoders = {}
    
    for col in categorical_columns:
        le = LabelEncoder()
        data[col] = le.fit_transform(data[col])
        label_encoders[col] = le  # Save the label encoder if you need to reverse the transformation later

    # Convert categorical columns to pandas Categorical type
    for col in categorical_columns:
        data[col] = pd.Categorical(data[col])

    # Define feature columns and target column
    target_column = 'Total_time'
    X = data.drop(columns=[target_column])
    y = data[target_column].astype(float)

    # Handle NaN values for numerical columns (not categorical ones)
    # For numerical columns, use median to fill NaN values
    numerical_columns = X.select_dtypes(include=[np.number]).columns
    X[numerical_columns] = X[numerical_columns].fillna(X[numerical_columns].median())

    # Handle NaN values for categorical columns (use the mode to fill missing values)
    categorical_columns = X.select_dtypes(include=['category']).columns
    for col in categorical_columns:
        mode_value = X[col].mode()[0]  # Get the most frequent category
        X[col] = X[col].fillna(mode_value)  # Fill missing values with the most frequent category

    # Split dataset into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Apply PCA
    pca = PCA(n_components=10)
    x_pca = pca.fit_transform(X_train)
    x_test_pca = pca.transform(X_test)  # Transform the test set as well

    # Initialize the XGBoost model for regression with enable_categorical=True
    model = XGBRegressor(n_estimators=100, random_state=42, enable_categorical=True)

    # Train the model with the PCA-transformed training data
    model.fit(x_pca, y_train)

    # Make predictions on the PCA-transformed test data
    y_pred = model.predict(x_test_pca)

    # Evaluate the model
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    # Display Model Performance Metrics Table
    metrics_df = pd.DataFrame({
        'Metric': ['Mean Absolute Error (MAE)', 'Mean Squared Error (MSE)', 'Root Mean Squared Error (RMSE)', 'R-squared (R²)'],
        'Value': [mae, mse, rmse, r2]
    })
    print(metrics_df)

    # Feature Importance Plot
    feature_importances = pd.DataFrame(model.feature_importances_,
                                    index=[f'PCA Component {i+1}' for i in range(x_pca.shape[1])],
                                    columns=['Importance']).sort_values('Importance', ascending=False)

    plt.figure(figsize=(10, 6))
    plt.barh(feature_importances.index, feature_importances['Importance'], color='skyblue')
    plt.xlabel('Importance')
    plt.title('Feature Importance')
    plt.gca().invert_yaxis()  # Invert y-axis for a descending order
    plt.show()






    # Actual vs. Predicted Plot
    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, y_pred, color='purple', alpha=0.6)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', linewidth=2)  # Diagonal line
    plt.xlabel('Actual Travel Time')
    plt.ylabel('Predicted Travel Time')
    plt.title('Actual vs. Predicted Travel Time')
    plt.show()

    # Residual Plot
    residuals = y_test - y_pred
    plt.figure(figsize=(8, 6))
    plt.scatter(y_pred, residuals, color='blue', alpha=0.6)
    plt.axhline(y=0, color='r', linestyle='--', linewidth=2)
    plt.xlabel('Predicted Travel Time')
    plt.ylabel('Residuals')
    plt.title('Residual Plot')
    plt.show()

    # Error Distribution Plot
    plt.figure(figsize=(8, 6))
    sns.histplot(residuals, kde=True, color='orange')
    plt.xlabel('Prediction Error')
    plt.title('Error Distribution')
    plt.show()

    # Predicted vs Actual Delivery Time Distribution
    plt.figure(figsize=(8, 6))
    sns.histplot(y_test, color='blue', label='Actual', kde=True, alpha=0.5)
    sns.histplot(y_pred, color='red', label='Predicted', kde=True, alpha=0.5)
    plt.xlabel('Delivery Time (Minutes)')
    plt.title('Predicted vs Actual Delivery Time Distribution')
    plt.legend()
    plt.show()

    return model



def add_restaurants(lat,lon):
    id=int(input("Enter the Restaurant_id : "))
    name=input("Enter the Restaurants Name : ")
    r_lat=lat
    r_lon=lon
    sql = "INSERT INTO restaurants (restaurant_id, restaurant_name,longitude,latitude) VALUES (%s, %s, %s, %s)"
    values = (id, name, r_lon, r_lat)
    cursor.execute(sql, values)
    conn.commit()  





def add_destiantion(lat,lon):
    id=int(input("Enter the Destination_id : "))
    name=input("Enter the Destination Address : ")
    r_lat=lat
    r_lon=lon
    sql = "INSERT INTO destination (destination_id , Destination_addre,longitude,latitude) VALUES (%s, %s, %s, %s)"
    values = (id, name, r_lon, r_lat)
    cursor.execute(sql, values)
    conn.commit()  







def get_weather_data(api_key,lat,lon):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key_ow}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        temp = data['main']['temp']
        humidity = data['main']['humidity']
        precipitation = data.get('rain', {}).get('1h', 0)  
        weather_desc = data['weather'][0]['description']
        return temp, humidity, precipitation, weather_desc
    else:
        return None, None, None, None
    



def get_tomtom_distance(api_key, origin, destination):
    # TomTom API URL for routing service
    url = f"https://api.tomtom.com/routing/1/calculateRoute/{origin}:{destination}/json?key={api_key}"

    # Send the GET request to TomTom API
    try:
        response = requests.get(url)
        response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx or 5xx)

        # Try to parse the response as JSON
        data = response.json()

        # Check if the response contains valid data
        if 'routes' in data and len(data['routes']) > 0:
            # Extract the distance from the response (in meters)
            distance_meters = data['routes'][0]['summary']['lengthInMeters']
            distance_km = distance_meters / 1000  # Convert meters to kilometers
            return distance_km
        else:
            print(f"Error: No route found for origin: {origin}, destination: {destination}")
            return None
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    except ValueError as json_err:
        print(f"JSON decode error: {json_err}")

    return None






#Base time without traffic

def get_travel_time(source_lat, source_lon, destination_lat, destination_lon):
    try:
        headers = {'Authorization': API_KEY, 'Content-Type': 'application/json'}
        payload = {
            "coordinates": [
                [source_lon, source_lat],
                [destination_lon, destination_lat]
            ]
        }
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        travel_time = data['routes'][0]['summary']['duration']  # Time in seconds
        return travel_time / 60  # Convert to minutes
    except Exception as e:
        print(f"Error: {e}")
        return None






def Fetch_values():
    cursor = conn.cursor()
    age = int(input("Enter the age : "))
    rating = float(input("Enter the rating : "))
    r_id = int(input("Enter the Restaurant_id: "))
    d_id = int(input("Enter the Destination id : "))


    tov=input("Enter the type of vehicle :- scooter / motorcycle / electric_scooter / bicycle  ")
    tof=input("Enter the type of food :-  snack / drinks  / buffet / meal ")
    

    query = f"SELECT longitude, latitude FROM restaurants WHERE restaurant_id = {r_id}"
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        restaurant_lon = result[0]  
        restaurant_lat = result[1]

    
    query = f"SELECT longitude, latitude FROM destination WHERE destination_id = {d_id}"
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        delivery_lon = result[0]  
        delivery_lat = result[1]

    origin = f"{restaurant_lat},{restaurant_lon}"
    destination = f"{delivery_lat},{delivery_lon}"
    distance = get_tomtom_distance(api_key_tt, origin, destination)

    temp, humidity, precipitation, weather_desc = get_weather_data(api_key_ow, delivery_lat, delivery_lon)


    # Split the origin and destination strings
    restaurant_lat, restaurant_lon = origin.split(",")
    delivery_lat, delivery_lon = destination.split(",")

    # Convert to appropriate types if needed (e.g., float for latitude and longitude)
    restaurant_lat = float(restaurant_lat)
    restaurant_lon = float(restaurant_lon)
    delivery_lat = float(delivery_lat)
    delivery_lon = float(delivery_lon)

    
    
    travel_time = get_travel_time(restaurant_lat,restaurant_lon,delivery_lat,delivery_lon)



    

    time_delay_weater=0
    time_delay_vehicle=0
    time_delay_food=0
    time_delay_traffic=0
    total_delay=0



# delay due to the vehicle type
    if(tov=="scooter"):
        time_delay_vehicle=(travel_time-2)*0
        time_delay_traffic=(travel_time-2)*0.10

    if(tov=="motorcycle"):
        time_delay_vehicle=(travel_time-2)*0
        time_delay_traffic=(travel_time-2)*0.05

    if(tov=="electric_scooter"):
        time_delay_vehicle=(travel_time-2)*0.03
        time_delay_traffic=(travel_time-2)*0.10

    if(tov=="bicycle"):
        time_delay_vehicle=(travel_time-2)*0.10
        time_delay_traffic=(travel_time-2)*0.15

    

#delay due to the food type


    if(tof=="snack"):
        time_delay_food=(travel_time-2)*0.05

    if(tof=="drinks"):
        time_delay_food=(travel_time-2)*0.05

    if(tof=="buffet"):
        time_delay_food=(travel_time-2)*0.08

    if(tof=="meal"):
        time_delay_food=(travel_time-2)*0.10






    #Delay due to weather


    if(weather_desc=="broken clouds"):
        time_delay_weater=(travel_time-2)*0.05


    if(weather_desc=="clear sky"):
        time_delay_weater=(travel_time-2)*0.00


    if(weather_desc=="few clouds"):
        time_delay_weater=(travel_time-2)*0.00


    if(weather_desc=="fog"):
        time_delay_weater=(travel_time-2)*0.15


    if(weather_desc=="haze"):
        time_delay_weater=(travel_time-2)*0.10


    if(weather_desc=="light rain"):
        time_delay_weater=(travel_time-2)*0.15


    if(weather_desc=="mist"):
        time_delay_weater=(travel_time-2)*0.10


    if(weather_desc=="moderate rain"):
        time_delay_weater=(travel_time-2)*0.30


    if(weather_desc=="overcast clouds"):
        time_delay_weater=(travel_time-2)*0.15


    if(weather_desc=="scattered clouds"):
        time_delay_weater=(travel_time-2)*0.0


    if(weather_desc=="smoke"):
        time_delay_weater=(travel_time-2)*0.15


    

    #total_delay=time_delay_food+time_delay_vehicle+time_delay_weater+time_delay_traffic
    total_delay=time_delay_traffic

    #total_time=travel_time+total_delay

    
    if(total_delay<=1):
        #traffic_label="very low"
        traffic_label=0


    if(total_delay>1 and total_delay <=3):
        #traffic_label="low"
        traffic_label=1


    if(total_delay >3 and total_delay<=4.5):
        #traffic_label="moderate"
        traffic_label=2


    if(total_delay>4.5):
        #traffic_label="high"
        traffic_label=3

   
    
    return age, rating, tov,tof,distance, temp, precipitation,humidity,weather_desc,traffic_label;


def Predict_xgboost(model, age, rating, distance, temp, precipitation, humidity, weather_desc, traffic_level, ty_food, ty_veh):
    import pandas as pd

    # Create DataFrame for the input
    new_input = pd.DataFrame({
        'Delivery_person_Age': [age],
        'Delivery_person_Ratings': [rating],
        'Type_of_order': [ty_food],
        'Type_of_vehicle': [ty_veh],
        'temperature': [temp],
        'humidity': [humidity],
        'precipitation': [precipitation],
        'weather_description': [weather_desc],
        'Distance (km)': [distance],
        'Traffic_labels': [traffic_level]
        
       
        
        
    })

    # Set categorical columns to category dtype
    categorical_columns = ['weather_description', 'Type_of_vehicle', 'Type_of_order']
    for col in categorical_columns:
        new_input[col] = new_input[col].astype('category')

    # Make prediction
    try:
        prediction = model.predict(new_input)
        print(f"Predicted Travel Time for the input: {prediction[0]:.2f} minutes")
    except Exception as e:
        print(f"Error during prediction: {e}")






#EDA()
#Handling_missing_values()
#Ordinal_Encoding()





#add_restaurants(30.340807698299233, 76.43478427438586)
#add_destiantion(30.351334617754073, 76.36462736281393)



age, rating,tov,tof, distance, temp, precipitation,humidity,weather_desc,traffic_level=Fetch_values()



#print(age)
#print(rating)
#print(tov)
#print(tof)
#print(distance)
#print(temp)
#print(precipitation)
#print(humidity)
#print(weather_desc)
#print(traffic_level)


model=Xgboost_without_pca()
#model=Xgboost_with_pca()




Predict_xgboost(model,age, rating, distance, temp, precipitation,humidity, weather_desc,traffic_level,tof,tov)  






cursor.close()
conn.close()


