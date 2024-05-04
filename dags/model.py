# import pandas as pd
# import boto3
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_squared_error
# from sklearn.preprocessing import LabelEncoder
# from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

# # Load your cleaned and consolidated data
# def load_data(filepath):
#         # Initialize S3 client
#     s3 = boto3.client('s3', 
#                       aws_access_key_id=AWS_ACCESS_KEY_ID, 
#                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
#     # Read CSV from S3
#     file_key = 'clean_store_transactions.csv'
#     obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
#     data = pd.read_csv(obj['Body'])
#     return data

# # Prepare your data for modeling
# def prepare_data(data):
#     # Handle categorical variables with label encoding
#     categorical_cols = data.select_dtypes(include=['object']).columns
#     le = LabelEncoder()
#     for col in categorical_cols:
#         data[col] = le.fit_transform(data[col])
    
#     # Handle Date column - extract potentially useful features
#     data['Date'] = pd.to_datetime(data['Date'])
#     data['Year'] = data['Date'].dt.year
#     data['Month'] = data['Date'].dt.month
#     data['Day'] = data['Date'].dt.day
#     data = data.drop('Date', axis=1)  # Now we can drop the original Date column

#     # Assume 'SP' column is the target for sales prediction
#     X = data.drop('SP', axis=1)
#     y = data['SP']
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
#     return X_train, X_test, y_train, y_test

# # Train the model
# def train_model(X_train, y_train):
#     model = RandomForestRegressor(n_estimators=100, random_state=42)
#     model.fit(X_train, y_train)
#     return model

# # Evaluate the model
# def evaluate_model(model, X_test, y_test):
#     predictions = model.predict(X_test)
#     mse = mean_squared_error(y_test, predictions)
#     rmse = mse ** 0.5
#     print(f'RMSE: {rmse}')
#     return rmse

# # Analyze and suggest improvements based on feature importances
# def analyze_and_suggest(model, features):
#     importances = pd.Series(model.feature_importances_, index=features.columns)
#     top_features = importances.nlargest(5)
#     print(f"Top 5 features affecting sales: {top_features}")
#     return top_features

# # Provide suggestions based on model accuracy and feature importances
# def provide_suggestions(rmse, y_test, top_features):
#     mean_sales = y_test.mean()
#     print(f"Average sales (SP): {mean_sales}")

#     if rmse < (0.1 * mean_sales):
#         print("The model's predictions are quite accurate, and you can rely on the model for forecasting future sales.")
#     elif rmse < (0.2 * mean_sales):
#         print("The model's predictions are reasonable, but there is room for improvement.")
#     else:
#         print("The model's predictions have a high error margin. Consider revisiting the features or model selection.")

#     print("\nTo improve sales predictions:")
#     print(f"Focus on the most influential factors affecting sales: {', '.join(top_features.index[:3])}.")
#     print("Consider reviewing the pricing strategy, as MRP and discounts are likely to be significant drivers of sales.")
#     print("Examine any external factors that may influence sales patterns, such as seasonal trends or marketing campaigns.")
    

# # Main function to run the model pipeline
# def main():
#     filepath = 'clean_store_transactions.csv'
#     data = load_data(filepath)
#     X_train, X_test, y_train, y_test = prepare_data(data)
#     model = train_model(X_train, y_train)
#     rmse = evaluate_model(model, X_test, y_test)
#     top_features = analyze_and_suggest(model, X_train)
#     provide_suggestions(rmse, y_test, top_features)

# if __name__ == "__main__":
#     main()
    
    
    
    
    
import pandas as pd
import boto3
from io import StringIO
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
from s3_details import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME

def predictions():
        # Function to upload a DataFrame to an S3 bucket as a CSV file
    def upload_to_s3(df, bucket, object_name, aws_access_key_id, aws_secret_access_key):
        # Create an S3 client
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload the CSV file
        response = s3_client.put_object(Bucket=bucket, Key=object_name, Body=csv_buffer.getvalue())
        return response

    # Function to train a Random Forest model and make predictions
    def train_and_predict(data):
        X = data.drop('SP', axis=1)
        y = data['SP']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        
        rmse = mean_squared_error(y_test, predictions, squared=False)
        print(f'RMSE: {rmse}')
        
        return y_test, predictions, model

    # Load the data
    # data = pd.read_csv('clean_store_transactions.csv')
    s3 = boto3.client('s3', 
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # Read CSV from S3
    file_key = 'clean_store_transactions.csv'
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = pd.read_csv(obj['Body'])
        
    # Encode categorical columns
    label_encoders = {}
    for column in data.select_dtypes(include=['object']).columns:
        label_encoders[column] = LabelEncoder()
        data[column] = label_encoders[column].fit_transform(data[column])

    # Add date features
    data['Date'] = pd.to_datetime(data['Date'])
    data['Year'] = data['Date'].dt.year
    data['Month'] = data['Date'].dt.month
    data['Day'] = data['Date'].dt.day
    data.drop('Date', axis=1, inplace=True)

    # Train the model and get predictions
    actual, predicted, model = train_and_predict(data)

    # Create a DataFrame for the actual and predicted values
    results_df = pd.DataFrame({'Actual': actual, 'Predicted': predicted})

    # Define your S3 bucket and object name
    bucket = BUCKET_NAME
    object_name = 'predicted_sales_results.csv'

    # AWS credentials (it's recommended to use a more secure method of storing them)
    aws_access_key_id = AWS_ACCESS_KEY_ID
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY

    # Upload the results to S3
    upload_response = upload_to_s3(results_df, bucket, object_name, aws_access_key_id, aws_secret_access_key)
    print(upload_response)

predictions()