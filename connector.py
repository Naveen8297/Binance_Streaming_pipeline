import websocket
import json
import socket
import time
import yaml

# set up the socket connection
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
server_address = ('localhost', 9093)  # Send the data from Binance stream to port 9093
sock.connect(server_address)  

def on_message(ws, message):
    
    # Parse the JSON message
    data = json.loads(message)
    # Print out the incoming message
    #print(data)
    
    sock.send(str(data).encode('utf-8'))
    #time.sleep(1)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("Connection closed")

def on_open(ws):
    # Subscribe to the Binance WebSocket stream for a specific symbol
    with open("config.yaml") as f:
       config = yaml.safe_load(f) 

    symbol = config['symbol']
    
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@bookTicker"], #for symbol in symbols],
        "id": 1
    }
    ws.send(json.dumps(subscribe_msg))

if __name__ == "__main__":
    # Set up the WebSocket connection
    ws_url = "wss://fstream.binance.com/ws"     #9443/ws"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    # Start the WebSocket connection
    ws.run_forever()
