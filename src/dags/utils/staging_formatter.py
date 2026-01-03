import pandas as pd

class AssetInfosFormatters :
    def format_crypto_infos(data) :

        data_struct = {
            "symbol" : data["symbol"],
            "crypto_name" : data["longName"],
            "region" : data["region"],
            "exchange_timezone" : data["exchangeTimezoneName"]
        }
        
        return pd.DataFrame([data_struct])

    def format_forex_infos(data) :
        print(f'DATA : {data}')
        data_struct = {
            "symbol" : data["symbol"],
            "forex_name" : data["longName"],
            "region" : data["region"],
            "exchange_timezone" : data["exchangeTimezoneName"]
        }
        
        return pd.DataFrame([data_struct])

    def format_index_infos(data) :
        name_key = 'longName' if 'longName' in data.keys() else 'shortName'

        currency = None
        if 'currency' not in data.keys() :
            currency = data['symbol'].split('-')[1]
        
        data_struct = {
            "symbol" : data["symbol"],
            "currency" : data["currency"] if currency is None else currency,
            "index_name" : data[name_key],
            "region" : data["region"],
            "exchange_timezone" : data["exchangeTimezoneName"]
        }
        
        return pd.DataFrame([data_struct])


    def format_futures_infos(data) :
        if 'shortName' in data.keys() :
            name_key = 'shortName'
        elif 'longName' in data.keys() :
            name_key = 'longName'
        else :
            name_key = 'fullExchangeName'

        data_struct = {
            "symbol" : data["symbol"],
            "currency" : data["currency"],
            "future_name" : data[name_key],
            "region" : data["region"],
            "exchange_timezone" : data["exchangeTimezoneName"]
        }
        
        return pd.DataFrame([data_struct])


class AssetHistoryFormatters :
    
    def format_asset_history(symbol:str, data) :
        print(f"DATA RECEIVED FOR FORMATTING: {data}")
        df = pd.DataFrame(data)
        df = df.rename(columns={
            "Date" : "value_timestamp",
            "Open" : "value_open",
            "Low" : "value_low",
            "High" : "value_high",
            "Close" : "value_close",
            "Dividends" : "dividends",
            "Volume" : "volume",
            "Stock Splits" : "stock_splits"
        })

        start = symbol.find('_')+1
        end = symbol.rfind('_')

        df["symbol"] = symbol[start:end]
        df["value_timestamp"] = df["value_timestamp"].astype(str)

        return df[['symbol', 'value_timestamp', 'value_open', 'value_high', 'value_low', 'value_close', 'volume', 'dividends']]
