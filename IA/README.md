# Phone Price Predictor

This project is a machine learning application that predicts the price of phones based on various features such as seller, model, age, and condition. The model is built using a Random Forest Regressor and is trained on a dataset of phone prices.

## Cleaning up dataset

Use parquet_compiler.py to compile the different .parquet into a .csv

Then use clean_parquet_csv.py to clean up the .csv

The file will be merged_data.csv

## Project Structure

```
IA
├── data
│   └── phone_prices.csv         # Dataset used for training the model
├── phone_price_predictor.py      # Main script for training and predicting
├── requirements.txt              # List of dependencies
└── README.md                     # Project documentation
```

## Dataset

The dataset is located in the `data/phone_prices.csv` file and includes the following features:
- **seller**: The seller of the phone.
- **model**: The model of the phone.
- **date**: The date when the phone was listed.
- **condition**: The condition of the phone (new or used).
- **price**: The price of the phone.

## Installation

To set up the environment, you need to install the required dependencies. You can do this by running:

```
pip install -r requirements.txt
```

## Usage

### Running the Script

You can run the main script to train the model from scratch or load a pre-trained model for retraining. Use the following command:

```
python phone_price_predictor.py
```

### Saving and Loading the Model

The script allows you to save the trained model to a file named `phone_price_model.joblib`. If you want to retrain the model using the existing data, simply run the script again, and it will load the pre-trained model if available.

### Outputs

- `phone_price_model.joblib`: Serialized trained model for future use or retraining.  
- `processed_data.csv`: The combined and featurized dataset (features + target) saved after each run.

### Making Predictions

Once the model is trained, you can use it to make predictions on new data. Ensure that the new data is preprocessed in the same way as the training data.
