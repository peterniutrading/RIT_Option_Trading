import re
import math
import numpy
import signal
import requests
from time import sleep
from scipy.stats import norm

def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

API_KEY = {'X-API-Key': 'zwang'}
shutdown = False


def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.ok:
        case = resp.json()
        return case['tick']

#  return the bid and ask price  for specific ticker at certian tick
def ticker_info(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities/book', params=payload)
    if resp.ok:
        book = resp.json()
        return book['bids'][0]['price'], book['asks'][0]['price']

# parameter d1 for BS model
def d1(stock, k, sigma, t):
    return (math.log(stock/k)+(0.5*(sigma**2))*t)/(sigma * math.sqrt(t))

# parameter d2 for BS model
def d2(stock, k, sigma, t):
    return d1(stock, k, sigma, t)-sigma*math.sqrt(t)

# delta of call option
def delta_call(stock, k, sigma, t):
    return norm.cdf(d1(stock, k, sigma, t))

# delta of put option
def delta_put(stock, k, sigma, t):
    return -norm.cdf(-d1(stock, k, sigma, t))

# BS model for call option
def bs_call(stock, k, sigma, t):
    return stock* norm.cdf(d1(stock, k, sigma, t))-k* norm.cdf(d2(stock, k, sigma, t))

# BS model for put option
def bs_put(stock, k, sigma, t):
    return -stock* norm.cdf(-d1(stock, k, sigma, t))+ k* norm.cdf(-d2(stock, k, sigma, t))

# Here, it's the signal for each individual option:
# Trading signal:
# ticker_bid price > 1.03 * ticker_ask theoretical pirce: sell this ticker
# 1.03 * ticker_ask price < ticker_bid theoretical price: buy this ticker
# Reason for '1.03' is to enlarge the mispricing spread
# 'Clear' the ticker if this ticker has position
# Use 'spread' to store the degree of mispricing ticker
def single_call_option_spread(session, ticker, k, sigma, t, rtm_bid, rtm_ask):
    ticker_bid, ticker_ask = ticker_info(session, ticker)
    ticker_bid_th = bs_call(rtm_bid, k, sigma, t)
    ticker_ask_th = bs_call(rtm_ask, k, sigma, t)
    if ticker_bid >1.03* ticker_ask_th:
        ticker_action = 'SELL'
        spread = ticker_bid - ticker_ask_th
    elif 1.03* ticker_ask < ticker_bid_th:
        ticker_action = 'BUY'
        spread = ticker_bid_th - ticker_ask
    else:
        ticker_action = 'CLEAR'
        spread = 0
    return ticker_action, ticker, spread

def single_put_option_spread(session, ticker, k, sigma, t, rtm_bid, rtm_ask):
    ticker_bid, ticker_ask = ticker_info(session, ticker)
    ticker_bid_th = bs_put(rtm_bid, k, sigma, t)
    ticker_ask_th = bs_put(rtm_ask, k, sigma, t)
    if ticker_bid >1.03* ticker_ask_th:
        ticker_action = 'SELL'
        spread = ticker_bid - ticker_ask_th
    elif 1.03* ticker_ask < ticker_bid_th:
        ticker_action = 'BUY'
        spread = ticker_bid_th - ticker_ask
    else:
        ticker_action = 'CLEAR'
        spread = 0
    return ticker_action, ticker, spread

# calculate the position of each ticker in the portfolio:
def position(session,ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities', params=payload)
    security = resp.json()
    return security[0]["position"]

# calculate the portfolio delta:
def port_delta(session, rtm, sigma, t):
    total_delta = 0
    for i in range(10):
        k = i + 45
        total_delta += position(session, f'RTM{k}C')* delta_call(rtm, k, sigma, t)* 100
        total_delta += position(session, f'RTM{k}P')* delta_put(rtm, k, sigma, t)* 100
    total_delta += position(session, 'RTM')
    return total_delta

#Clear the position to be zero, but this function only works for option Case
#Use RTM security to realize the delta limit later
def clear_position(session, ticker):
    ticker_position = position(session, ticker)
    number_of_times = abs(ticker_position) // 100
    if ticker_position >0:
        for i in range(int(number_of_times)):
            session.post('http://localhost:9999/v1/orders', params={'ticker': ticker, 'type': 'MARKET', 'quantity': 100, 'action': 'SELL'})
        residual = ticker_position - number_of_times * 100
        if residual >0:
            session.post('http://localhost:9999/v1/orders', params={'ticker': ticker, 'type': 'MARKET', 'quantity': residual, 'action': 'SELL'})
    if ticker_position <0:
        for i in range(int(number_of_times)):
            session.post('http://localhost:9999/v1/orders', params={'ticker': ticker, 'type': 'MARKET', 'quantity': 100, 'action': 'BUY'})
        residual = ticker_position + number_of_times * 100
        if residual <0:
            session.post('http://localhost:9999/v1/orders', params={'ticker': ticker, 'type': 'MARKET', 'quantity': -residual, 'action': 'BUY'})

#This function aims to decrease delta to zero using RTM security
def delta_hedge(session, total_delta):
    number_of_securities = int(total_delta)
    if number_of_securities > 0:
        number_of_times = number_of_securities // 10000
        for i in range(number_of_times):
            session.post('http://localhost:9999/v1/orders', params={'ticker': 'RTM', 'type': 'MARKET', 'quantity': 10000, 'action': 'SELL'})
        residual = number_of_securities - number_of_times * 10000
        if residual >0:
            session.post('http://localhost:9999/v1/orders', params={'ticker': 'RTM', 'type': 'MARKET', 'quantity': residual, 'action': 'SELL'})
    if number_of_securities < 0:
        number_of_securities = -number_of_securities
        number_of_times = number_of_securities // 10000
        for i in range(number_of_times):
            session.post('http://localhost:9999/v1/orders', params={'ticker': 'RTM', 'type': 'MARKET', 'quantity': 10000, 'action': 'BUY'})
        residual = number_of_securities - number_of_times * 10000
        if residual >0:
            session.post('http://localhost:9999/v1/orders', params={'ticker': 'RTM', 'type': 'MARKET', 'quantity': residual, 'action': 'BUY'})

# Return volatility from the latest news:
def get_volatility(session,i):
    resp = session.get('http://localhost:9999/v1/news?since={}'.format(i))
    news = resp.json()
    body = news[0]['body']
    volatility = [0.01*int(s[:-1]) for s in re.findall(r'\d+%', body)]
    return volatility

def main():
    with requests.Session() as s:
        s.headers.update(API_KEY)
        tick = get_tick(s)
        total_delta = 0
        news = s.get('http://localhost:9999/v1/news').json()
        while tick > 0 and tick < 600 and not shutdown:
            if (len(news)<3):
                sigma = 0.2
            else:
                sigma = numpy.mean(get_volatility(s,len(news)-2))

            t = (600-tick)/(30*240)

            #Update RTM price
            rtm_bid, rtm_ask = ticker_info(s,'RTM')
            rtm = (rtm_bid+rtm_ask)/2
            call_list = [1]*10
            put_list = [1]*10

            #Do the delta hedging
            total_delta = port_delta(s, rtm, sigma, t)
            delta_hedge(s, total_delta)

            #Update option prices & Clear position if it shows
            for i in range(10):
                k = i + 45
                call_list[i] = list(single_call_option_spread(s, f'RTM{k}C', k, sigma, t, rtm_bid, rtm_ask))
                if call_list[i][0] == 'CLEAR' and  abs(position(s, f'RTM{k}C'))>0:
                    clear_position(s, f'RTM{k}C')
                put_list[i] = list(single_put_option_spread(s, f'RTM{k}P', k, sigma, t, rtm_bid, rtm_ask))
                if put_list[i][0] == 'CLEAR' and abs(position(s, f'RTM{k}P'))>0:
                    clear_position(s, f'RTM{k}P')

            #Execute the strategy:
            #Rank the spread from high to low
            total_list = call_list + put_list
            total_list.sort(key= lambda x: x[2], reverse=True)

            #We do the maximize volume for top mispricing options (only execute first 5 elements)
            for element in total_list[:5]:
                s.post('http://localhost:9999/v1/orders', params={'ticker': element[1], 'type': 'MARKET', 'quantity': 100, 'action': element[0]})

            sleep(1)
            tick = get_tick(s)
            news = s.get('http://localhost:9999/v1/news').json()

if __name__ == '__main__':
    # register the custom signal handler for graceful shutdowns
    signal.signal(signal.SIGINT, signal_handler)
    main()
