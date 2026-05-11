'''[출처] https://github.com/highfestiva/finplot/blob/master/finplot/examples/complicated.py
   examples/complicated.py 
   ※ 원래 코드(실시간 차트 정보 수신 잘 됨.) => x.y grid 표시, 좌측면 가격레이블 표시 개선
      cross_hair_color 색상 조정(흰색)- 완성(23.1.26.)
   => 위 코드에서 binance orderbook 및 trade 정보를 웹소켓으로 받는 것으로 수정 시도(23.2.24)

   '''
#

import finplot as fplt
from functools import lru_cache
import json
from math import nan
import pandas as pd
from PyQt6.QtWidgets import QApplication, QWidget, QPushButton, QLineEdit, QInputDialog  #text_input시 QInputDialog 사용
from PyQt6.QtWidgets import QComboBox, QCheckBox, QWidget, QGridLayout
from PyQt6.QtGui import QShortcut
import sys
import clipboard   # clipboard에 복사, 붙여넣기 기능
import pyqtgraph as pg
import requests
from time import time as now, sleep
from threading import Thread
import websocket
from datetime import datetime, timezone, timedelta
from ta.trend import ADXIndicator
import numpy as np

# CCXT를 이용한 실질화를 위해 수입한 코드=======================
import ccxt
import config_yaml_manager as cm     #sa

# data source 얻는 방식 선택
 # get_source_way='csv'   # 'csv'  / 'connect' 중 선택 설정
api_key=cm.APP_KEY
secret=cm.APP_SECRET

#binance 객체 생성
exchange = ccxt.binance(config={
    'apiKey' : api_key,
    'secret' : secret,
    'enableRateLimit' : True})
# 'options': {'defaultType': 'future'}  #선물 거래시
# ==============================================================

class BinanceFutureWebsocket:
    def __init__(self):
        self.url = 'wss://fstream.binance.com/stream'
        self.symbol = None
        self.interval = None
        self.ws = None
        self.df = None

    def reconnect(self, symbol, interval, df):
        '''Connect and subscribe, if not already done so.'''
        self.df = df
        if symbol.lower() == self.symbol and self.interval == interval:
            return
        self.symbol = symbol.lower()
        self.interval = interval
        self.thread_connect = Thread(target=self._thread_connect)
        self.thread_connect.daemon = True
        self.thread_connect.start()

    def close(self, reset_symbol=True):
        if reset_symbol:
            self.symbol = None
        if self.ws:
            self.ws.close()
        self.ws = None

    def _thread_connect(self):
        self.close(reset_symbol=False)
        print('websocket connecting to %s...' % self.url)
        self.ws = websocket.WebSocketApp(self.url, on_message=self.on_message, on_error=self.on_error)
        self.thread_io = Thread(target=self.ws.run_forever)
        self.thread_io.daemon = True
        self.thread_io.start()
        for _ in range(100):
            if self.ws.sock and self.ws.sock.connected:
                break
            sleep(0.1)
        else:
            self.close()
            raise websocket.WebSocketTimeoutException('websocket connection failed')
        self.subscribe(self.symbol, self.interval)
        print('websocket connected')

    def subscribe(self, symbol, interval):
        try:
            data = '{"method":"SUBSCRIBE","params":["%s@kline_%s"],"id":1}' % (symbol, interval)
            self.ws.send(data)
        except Exception as e:
            print('websocket subscribe error:', type(e), e)
            raise e

    def on_message(self, *args, **kwargs):
        df = self.df
        if df is None:
            return
        msg = json.loads(args[-1])
        if 'stream' not in msg:
            return
        stream = msg['stream']
        #print(stream)
        if '@kline_' in stream:
            k = msg['data']['k']
            #print(msg) : {'stream': 'ethusdt@kline_1m', 'data': {'e': 'kline', 'E': 1674627097088, 's': 'ETHUSDT', 
            # 'k': {'t': 1674627060000, 'T': 1674627119999, 's': 'ETHUSDT', 'i': '1m', 'f': 2646911922, 'L': 2646913123, 
            # 'o': '1547.04', 'c': '1545.43', 'h':'1547.04', 'l': '1545.26', 'v': '1769.100', 'n': 1202, 'x': False, 
            # 'q': '2735118.54058', 'V': '162.023', 'Q': '250437.53605', 'B': '0'}}}
            t = k['t']
            # print('t:', t)
            # print('df.index[-1]', df.index[-2])
            # print('df.index[-1].timestamp()', df.index[-2].timestamp())
            # print('df.index[-1].timestamp()*1000', df.index[-2].timestamp()*1000)
            # # t: 1725515040000
            # # df.index[-2] 2024-09-05 05:43:00
            # # df.index[-2].timestamp() 1725514980.0
            # # df.index[-2].timestamp()*1000 1725514980000.0

            t0 = int(df.index[-2].timestamp()) * 1000
            t1 = int(df.index[-1].timestamp()) * 1000
            t2 = t1 + (t1-t0)
            if t < t2: #기존 시간이면
                # update last candle
                i = df.index[-1]
                df.loc[i, 'Close']  = float(k['c'])
                df.loc[i, 'High']   = max(df.loc[i, 'High'], float(k['h']))
                df.loc[i, 'Low']    = min(df.loc[i, 'Low'],  float(k['l']))
                df.loc[i, 'Volume'] = float(k['v'])
            else:
                # create a new candle
                data = [t] + [float(k[i]) for i in ['o','c','h','l','v']]
                candle = pd.DataFrame([data], columns='Time Open Close High Low Volume'.split()).astype({'Time':'datetime64[ms]'})
                candle.set_index('Time', inplace=True)
                self.df = pd.concat([df, candle])

    def on_error(self, error, *args, **kwargs):
        print('websocket error: %s' % error)


def do_load_price_history(symbol, interval):
    url = 'https://www.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=%s' % (symbol, interval, 1000)
    print('loading binance future %s %s' % (symbol, interval))
    d = requests.get(url).json()
    df = pd.DataFrame(d, columns='Time Open High Low Close Volume a b c d e f'.split())
    df = df.astype({'Time':'datetime64[ms]', 'Open':float, 'High':float, 'Low':float, 'Close':float, 'Volume':float})
    return df.set_index('Time')


@lru_cache(maxsize=5)
def cache_load_price_history(symbol, interval):
    '''Stupid caching, but works sometimes.'''
    return do_load_price_history(symbol, interval)


def load_price_history(symbol, interval):
    '''Use memoized, and if too old simply load the data.'''
    df = cache_load_price_history(symbol, interval)
    print('df=======================',df)
    # check if cache's newest candle is current
    t0 = df.index[-2].timestamp()
    t1 = df.index[-1].timestamp()
    t2 = t1 + (t1 - t0)
    if now() >= t2:
        df = do_load_price_history(symbol, interval)
    return df


def calc_parabolic_sar(df, af=0.2, steps=10):
    up = True
    sars = [nan] * len(df)
    sar = ep_lo = df.Low.iloc[0]
    ep = ep_hi = df.High.iloc[0]
    aaf = af
    aaf_step = aaf / steps
    af = 0
    for i,(hi,lo) in enumerate(zip(df.High, df.Low)):
        # parabolic sar formula:
        sar = sar + af * (ep - sar)
        # handle new extreme points
        if hi > ep_hi:
            ep_hi = hi
            if up:
                ep = ep_hi
                af = min(aaf, af+aaf_step)
        elif lo < ep_lo:
            ep_lo = lo
            if not up:
                ep = ep_lo
                af = min(aaf, af+aaf_step)
        # handle switch
        if up:
            if lo < sar:
                up = not up
                sar = ep_hi
                ep = ep_lo = lo
                af = 0
        else:
            if hi > sar:
                up = not up
                sar = ep_lo
                ep = ep_hi = hi
                af = 0
        sars[i] = sar
    df['sar'] = sars
    return df['sar']


def calc_rsi(price, n=14, ax=None):
    diff = price.diff().values
    gains = diff
    losses = -diff
    gains[~(gains>0)] = 0.0
    losses[~(losses>0)] = 1e-10 # we don't want divide by zero/NaN
    m = (n-1) / n
    ni = 1 / n
    g = gains[n] = gains[:n].mean()
    l = losses[n] = losses[:n].mean()
    gains[:n] = losses[:n] = nan
    for i,v in enumerate(gains[n:],n):
        g = gains[i] = ni*v + m*g
    for i,v in enumerate(losses[n:],n):
        l = losses[i] = ni*v + m*l
    rs = gains / losses
    rsi = 100 - (100/(1+rs))
    return rsi

def bollenger_band(df):
    mean= df.Close.rolling(50).mean()   
    stddev= df.Close.rolling(50).std()   

    boll_hi = mean + 2.5*stddev
    boll_lo = mean - 2.5*stddev
    boll_hi_max = boll_hi.rolling(50).max(),
    boll_lo_min = boll_lo.rolling(50).min(),
    
    return boll_hi, boll_lo, boll_hi_max, boll_lo_min


def calc_stochastic_oscillator(df, n=14, m=3, smooth=3):
    lo = df.Low.rolling(n).min()
    hi = df.High.rolling(n).max()
    k = 100 * (df.Close-lo) / (hi-lo)
    d = k.rolling(m).mean()
    return k, d

# def calculate_adx_all(df, window=14):
#     adx = ADXIndicator(high=df['High'], low=df['Low'], close=df['Close'], window=window)
#     return adx.adx(), adx.adx_pos(), adx.adx_neg()


def calc_plot_data(df, indicators):
    '''Returns data for all plots and for the price line.'''
    price = df['Open Close High Low'.split()]
    volume = df['Open Close Volume'.split()]
    ma50 = ma200 = vema24 = sar = rsi = stoch = stoch_s = None
    ma5_max=ma5_min=above_70=below_30=None
    boll_hi = boll_lo = boll_hi_max = boll_lo_min = None 
    if 'few' in indicators or 'moar' in indicators:
        ma3  = price.Close.rolling(3).mean()
        ma5  = price.Close.rolling(5).mean()
        hi5   = price.High.rolling(3).mean()
        ma10  = price.Close.rolling(10).mean()
        ma50  = price.Close.rolling(50).mean()
        ma200 = price.Close.rolling(200).mean()
        vema24 = volume.Volume.ewm(span=24).mean()
        ma5_max=ma5.rolling(20).max()
        # print(f'ma5_max: {ma5_max}')
        ma5_min=ma5.rolling(20).min()

    if 'moar' in indicators:
        sar = calc_parabolic_sar(df)
        rsi = calc_rsi(df.Close)
        # above_70 = rsi.where(rsi >= 70)
        above_70 = np.where(rsi >= 70, rsi, np.nan)  # 조건을 만족하지 않는 값은 NaN으로 대체
        below_30 = np.where(rsi <= 30, rsi, np.nan)  # 조건을 만족하지 않는 값은 NaN으로 대체
        boll_hi, boll_lo, boll_hi_max, boll_lo_min = bollenger_band(df)
        stoch,stoch_s = calc_stochastic_oscillator(df)

    plot_data = dict(price=price, volume=volume, ma3=ma3, ma5=ma5, hi5=hi5, ma10=ma10, ma50=ma50, \
                    ma200=ma200, vema24=vema24, sar=sar, rsi=rsi, above_70=above_70, below_30=below_30, \
                    stoch=stoch, stoch_s=stoch_s, boll_hi=boll_hi, \
                    boll_lo=boll_lo, boll_hi_max=boll_hi_max, boll_lo_min=boll_lo_min, \
                    ma5_max=ma5_max, ma5_min=ma5_min)
    # for price line
    last_close = price.iloc[-1].Close
    last_col = fplt.candle_bull_color if last_close > price.iloc[-2].Close else fplt.candle_bear_color
    price_data = dict(last_close=last_close, last_col=last_col)
    return plot_data, price_data


def realtime_update_plot():
    '''Called at regular intervals by a timer.'''
    if ws.df is None:
        return

    # calculate the new plot data
    global data
    indicators = ctrl_panel.indicators.currentText().lower()
    data,price_data = calc_plot_data(ws.df, indicators)
    

    # first update all data, then graphics (for zoom rigidity)
    #print(data)
    for k in data:
        #print(k)
        #print(data[k])
        #print(data)
        if data[k] is not None:
            plots[k].update_data(data[k], gfx=False)
    # for k in data:  #왜 반복하고 있는건지?(24.8)
    #     if data[k] is not None:
            plots[k].update_gfx()

    # place and color price line
    ax.price_line.setPos(price_data['last_close'])
    ax.price_line.pen.setColor(pg.mkColor(price_data['last_col']))


def change_asset(*args, **kwargs):
    '''Resets and recalculates everything, and plots for the first time.'''
    global data
    # save window zoom position before resetting
    fplt._savewindata(fplt.windows[0])

    # default to local timezone(한국시간을 한국시간으로 표시하기 위해 timezone.utc를 사용)
    # fplt.display_timezone = datetime.now(timezone.utc).astimezone(timezone.utc).tzinfo  
    fplt.display_timezone = datetime.now(timezone.utc).astimezone().tzinfo  #default to local

    symbol = ctrl_panel.symbol.currentText()
    interval = ctrl_panel.interval.currentText()

    
    ws.close()
    ws.df = None
    df = load_price_history(symbol, interval=interval)
    ws.reconnect(symbol, interval, df)

    # remove any previous plots
    ax.reset()
    axo.reset()
    ax_rsi.reset()
    # add x.y grid lind===================================================
    ax.set_visible(xgrid=True, ygrid=True)   #다른 코드에서 참고, 추가 코드

    # calculate plot data
    global data
    indicators = ctrl_panel.indicators.currentText().lower()
    data,price_data = calc_plot_data(df, indicators)
    
    # some space for legend
    ctrl_panel.move(100 if 'clean' in indicators else 200, 0)

    # plot data
    global plots
    plots = {}
    plots['price'] = fplt.candlestick_ochl(data['price'], ax=ax)
    plots['volume'] = fplt.volume_ocv(data['volume'], ax=axo)
    if data['ma50'] is not None:
        plots['ma3'] = fplt.plot(data['ma3'], legend='MA-3', ax=ax)
        plots['ma5'] = fplt.plot(data['ma5'], legend='MA-5', ax=ax)
        plots['hi5'] = fplt.plot(data['hi5'], style='.', legend='HI-5', ax=ax)
        plots['ma10'] = fplt.plot(data['ma10'], width=2.5,  legend='MA-10', ax=ax)
        plots['ma50'] = fplt.plot(data['ma50'],  width=3.5, style='-', legend='MA-50', ax=ax)
        plots['ma200'] = fplt.plot(data['ma200'], width=4.5,  style='-', legend='MA-200', ax=ax)
        plots['vema24'] = fplt.plot(data['vema24'], color=4, legend='V-EMA-24', ax=axo)
        # max_min line plot
        plots['ma5_max'] = fplt.plot(data['ma5_max'], style='.', width=1.5, color='green', legend='ma5_max', ax=ax)
        plots['ma5_min'] = fplt.plot(data['ma5_min'], style='.', width=1.5, color='green', legend='ma5_min', ax=ax)
    
    if data['rsi'] is not None:
        ax.set_visible(xaxis=False)
        ax_rsi.show()
        fplt.set_y_range(0, 100, ax=ax_rsi)
        fplt.add_band(30, 70, color='#6335', ax=ax_rsi)
        plots['rsi'] = fplt.plot(data['rsi'], legend='RSI', ax=ax_rsi)

        # RSI가 70 이상인 영역을 빨간색으로 표시
        # above_70 = data['rsi'].where(data['rsi'] >= 70)
        plots['above_70'] = fplt.plot(data['above_70'], color='#FF0000', style='+', width=1.5, ax=ax_rsi)  # 빨간색으로 설정

        # RSI가 30 이하인 영역을 빨간색으로 표시
        # below_30 = data['rsi'].where(data['rsi'] <= 30)
        plots['below_30'] = fplt.plot(data['below_30'], color='white', style='+', width=1.5, ax=ax_rsi)  # 빨간색으로 설정

        plots['stoch'] = fplt.plot(data['stoch'], color='#880', legend='Stoch', ax=ax_rsi)
        plots['stoch_s'] = fplt.plot(data['stoch_s'], color='#650', ax=ax_rsi)
        # plots['ADX'] = fplt.plot(data['adx'], width=2.5, color='aqua', legend='ADX',ax=ax_rsi)
        # plots['DI+'] = fplt.plot(data['dip'], width=1.5, color="lime", legend='DI+',ax=ax_rsi)
        # plots['DI-'] = fplt.plot(data['dim'], width=1.5, color='red', legend='DI-',ax=ax_rsi)

        plots['sar'] = fplt.plot(data['sar'], color='white', style='+', width=0.6, legend='SAR', ax=ax)   #color='#55a'
        p0=plots['boll_hi'] = fplt.plot(data['boll_hi'], color='#808080', legend='BB', ax=ax)
        p1=plots['boll_lo'] = fplt.plot(data['boll_lo'], color='#808080', legend='BB', ax=ax)
        fplt.fill_between(p0, p1, color='#072D2E')   # dark mode : '#072D2E')
        plots['boll_hi_max'] = fplt.plot(data['boll_hi_max'], style='.', width=1.5, color='cornflowerblue', legend='boll_hi_max', ax=ax)
        plots['boll_lo_min'] = fplt.plot(data['boll_lo_min'], style='.', width=1.5, color='cornflowerblue', legend='boll_lo_min', ax=ax)
        

    else:
        ax.set_visible(xaxis=True)
        ax_rsi.hide()

    # price line
    ax.price_line = pg.InfiniteLine(angle=0, movable=False, pen=fplt._makepen(fplt.candle_bull_body_color, style='.'))
    ax.price_line.setPos(price_data['last_close'])
    ax.price_line.pen.setColor(pg.mkColor(price_data['last_col']))
    ax.addItem(ax.price_line, ignoreBounds=True)

    for k in data:
        print(f"data[{k}]")
        print(data[k])

    # restores saved zoom position, if in range
    fplt.refresh()


def dark_mode_toggle(dark):
    '''Digs into the internals of finplot and pyqtgraph to change the colors of existing
       plots, axes, backgronds, etc.'''
    # first set the colors we'll be using
    if dark:
        fplt.foreground = '#777'
        fplt.background = '#090c0e'
        fplt.candle_bull_color = fplt.candle_bull_body_color = '#0b0'
        fplt.candle_bear_color = '#a23'
        volume_transparency = '6'
    else:
        fplt.foreground = '#444'
        fplt.background = fplt.candle_bull_body_color = '#fff'
        fplt.candle_bull_color = '#380'
        fplt.candle_bear_color = '#c50'
        volume_transparency = 'c'
    fplt.volume_bull_color = fplt.volume_bull_body_color = fplt.candle_bull_color + volume_transparency
    fplt.volume_bear_color = fplt.candle_bear_color + volume_transparency
    #cross_hair_color 색상 조정
    fplt.cross_hair_color = '#FFFF'         #fplt.foreground+'8'
    fplt.draw_line_color = '#FFFF'#흰색(선그릴때)            #'#FFFF'          #'#888'
    fplt.draw_done_color = '#FFFF'#v퍼플색(선완료시)       '#FFFF00'#노랑색 '#FF00FF'#v퍼플색     #'#555'

    pg.setConfigOptions(foreground=fplt.foreground, background=fplt.background)
    # control panel color
    if ctrl_panel is not None:
        p = ctrl_panel.palette()
        p.setColor(ctrl_panel.darkmode.foregroundRole(), pg.mkColor(fplt.foreground))
        ctrl_panel.darkmode.setPalette(p)

    # window background
    for win in fplt.windows:
        win.setBackground(fplt.background)

    # axis, crosshair, candlesticks, volumes
    axs = [ax for win in fplt.windows for ax in win.axs]
    vbs = set([ax.vb for ax in axs])
    axs += fplt.overlay_axs
    axis_pen = fplt._makepen(color=fplt.foreground)
    for ax in axs:
        # ax.axes['right']['item'].setPen(axis_pen)
        # ax.axes['right']['item'].setTextPen(axis_pen)
        """좌측면에 가격레이블 표시(ax.axes['left'])""" #'right'로 설정시 가격이 표시되지 않음.
        ax.axes['left']['item'].setPen(axis_pen)  #sm
        ax.axes['left']['item'].setTextPen(axis_pen) #sm
        
        ax.axes['bottom']['item'].setPen(axis_pen)
        ax.axes['bottom']['item'].setTextPen(axis_pen)
        if ax.crosshair is not None:
            ax.crosshair.vline.pen.setColor(pg.mkColor(fplt.foreground))
            ax.crosshair.hline.pen.setColor(pg.mkColor(fplt.foreground))
            #cross_hair_color 색상 조정
            ax.crosshair.xtext.setColor(fplt.cross_hair_color)          #(fplt.foreground)
            ax.crosshair.ytext.setColor(fplt.cross_hair_color)          #(fplt.foreground)
        for item in ax.items:
            if isinstance(item, fplt.FinPlotItem):
                isvolume = ax in fplt.overlay_axs
                if not isvolume:
                    item.colors.update(
                        dict(bull_shadow      = fplt.candle_bull_color,
                             bull_frame       = fplt.candle_bull_color,
                             bull_body        = fplt.candle_bull_body_color,
                             bear_shadow      = fplt.candle_bear_color,
                             bear_frame       = fplt.candle_bear_color,
                             bear_body        = fplt.candle_bear_color))
                else:
                    item.colors.update(
                        dict(bull_frame       = fplt.volume_bull_color,
                             bull_body        = fplt.volume_bull_body_color,
                             bear_frame       = fplt.volume_bear_color,
                             bear_body        = fplt.volume_bear_color))
                item.repaint()
def update_crosshair_text(x, y, xtext, ytext):   # 추가한 기능(다른 예제에서 참고, 추가)
    # ytext에 ohlc값을 맵핑하기 위해 추가한 코드(차트가 추가될 경우에도 정확한 값을 전달, OK)
    # print(x, xtext, 'type(xtext): ',type(xtext))
    # print(datetime.strptime(xtext, '%Y-%m-%d %H:%M').timestamp())
    # print(ws.df.index[x])
    # print(ws.df.index[x].timestamp())
    # print(datetime.fromtimestamp(ws.df.index[x].timestamp()).strftime("%Y.%m.%d %H:%M"))
    #print(y, ytext)

    global pos_x, pos_y
    pos_x=x
    pos_y=y
    txt_ohlc=None
    row = ws.df.iloc[x]
    prerw= ws.df.iloc[x-1]
    # print("data['vema24']", data['vema24'][x])
    # print(data)
    vema=data['vema24'][x]
    if data['rsi'] is not None:
        rsi=data['rsi'][x]
        stoch=data['stoch'][x]
        stoch_s=data['stoch_s'][x]
        boll_hi=data['boll_hi'][x]
        boll_lo=data['boll_lo'][x]
    # print(stoch)
    # print(stoch_s)
    # print(boll_hi)
    # print(boll_lo)
    
    O=row.Open; C=row.Close
    H=row.High; L=row.Low; V=row.Volume
    pC=prerw.Close; pV=prerw.Volume
    if data['rsi'] is not None:
        txt_ohlc=f'      O {O:,.2f}(open대%)(전Close대%)\n' \
                    f'      C {C:,.2f}({C-O:+,.2f}, {(C-O)/O*100:>+5.2f}%)(p{C-pC:+,.2f}, {(C-pC)/pC*100:5.2f}%)\n' \
                    f'      H {H:,.2f}({H-O:+,.2f}, {(H-O)/O*100:>+5.2f}%)(P{H-pC:+,.2f}, {(H-pC)/pC*100:5.2f}%)\n' \
                    f'      L {L:,.2f}({L-O:+,.2f}, {(L-O)/O*100:>+5.2f}%)(P{L-pC:+,.2f}, {(L-pC)/pC*100:5.2f}%)\n' \
                    f'      V {V:,.0f}(P{V-pV:+,.0f}, av{vema:,.2f}, {V/vema*100:>+5.2f}%)\n' \
                    f'      rsi         {rsi:>,.2f}\n' \
                    f'      stoch       {stoch:>,.2f} st_s/ {stoch-stoch_s:+.2f}\n' \
                    f'      boll_hi     {boll_hi:>,.2f}\n' \
                    f'      boll_lo     {boll_lo:>,.2f}\n' \
                    f'      {x, xtext}'                    #(p{prev.dtime})'
    else:
        txt_ohlc=f'      O {O:,.2f}(open대%)(전Close대%)\n' \
                    f'      C {C:,.2f}({C-O:+,.2f}, {(C-O)/O*100:>+5.2f}%)(p{C-pC:+,.2f}, {(C-pC)/pC*100:5.2f}%)\n' \
                    f'      H {H:,.2f}({H-O:+,.2f}, {(H-O)/O*100:>+5.2f}%)(P{H-pC:+,.2f}, {(H-pC)/pC*100:5.2f}%)\n' \
                    f'      L {L:,.2f}({L-O:+,.2f}, {(L-O)/O*100:>+5.2f}%)(P{L-pC:+,.2f}, {(L-pC)/pC*100:5.2f}%)\n' \
                    f'      V {V:,.0f}(P{V-pV:+,.0f}, av{vema:,.2f}, {V/vema*100:>+5.2f}%)\n' \
                    f'      {x, xtext}'                    #(p{prev.dtime})'
        # txt_ohlc=f'      O {O:,.4f}\n' \
        #         f'      C {C:,.4f}({C-O:,.2f}, {(C-O)/O*100:.2f}%)\n' \
        #         f'      H {H:,.4f}({H-O:,.2f}, {(H-O)/O*100:.2f}%)\n' \
        #         f'      L {L:,.4f}({L-O:,.2f}, {(L-O)/O*100:.2f}%)\n' \
        #         f'      V {V:,.4f}(p{V-pV:,.2f}\n' \
        #         f'      {row.iloc[0].dtime}(p{prev.iloc[0].dtime})'
        
    ytext = f'{y:,.2f}\n' + txt_ohlc
    return xtext, ytext


def create_ctrl_panel(win):
    panel = QWidget(win)
    panel.move(100, 0)
    win.scene().addWidget(panel)
    layout = QGridLayout(panel)

    panel.symbol = QComboBox(panel)
    # [panel.symbol.addItem(i+'USDT') for i in 'BTC ETH XRP DOGE BNB SOL ADA LTC LINK DOT TRX BCH'.split()]
    [panel.symbol.addItem(i) for i in 'BTCUSDT ETHUSDT XRPUSDT DOGEUSDT BNBUSDT SOLUSDT ADAUSDT LTCUSDT LINKUSDT DOTUSDT TRXUSDT BCHUSDT'.split()]
    panel.symbol.setCurrentIndex(0)
    layout.addWidget(panel.symbol, 0, 0)
    panel.symbol.currentTextChanged.connect(change_asset)
    layout.setColumnMinimumWidth(0, 20)

    panel.interval = QComboBox(panel)
    [panel.interval.addItem(i) for i in '1d 4h 1h 30m 15m 5m 1m'.split()]
    panel.interval.setCurrentIndex(6)
    layout.addWidget(panel.interval, 0, 1)
    panel.interval.currentTextChanged.connect(change_asset)
    #layout.setColumnMinimumWidth(3, 30)
    layout.setColumnMinimumWidth(1, 30)

    panel.indicators = QComboBox(panel)
    [panel.indicators.addItem(i) for i in 'Clean:Few indicators:Moar indicators'.split(':')]
    panel.indicators.setCurrentIndex(1)
    layout.addWidget(panel.indicators, 0, 2)
    panel.indicators.currentTextChanged.connect(change_asset)
    layout.setColumnMinimumWidth(2, 150)

    #===<선_텍스트 추가>==============================================================
    panel.button_input = QPushButton(panel)
    panel.button_input.setText('input_Text')
    panel.button_input.setShortcut('Ctrl+T')
    panel.button_input.clicked.connect(ex.text_input)
    layout.addWidget(panel.button_input, 0, 3)
    #layout.setColumnMinimumWidth(3, 30)
    
    panel.button_hline = QPushButton(panel)
    panel.button_hline.setText('hline')
    panel.button_hline.setShortcut('Ctrl+Space')  #안먹힘?
    panel.button_hline.clicked.connect(ex.draw_hline)
    layout.addWidget(panel.button_hline, 0, 3)
    # layout.setColumnMinimumWidth(4, 2)

    
    panel.button_hlongline = QPushButton(panel)
    panel.button_hlongline.setText('hlongline')
    panel.button_hlongline.setShortcut('Ctrl+Shift+Space') #안먹힘?
    panel.button_hlongline.clicked.connect(ex.draw_hlongline)
    layout.addWidget(panel.button_hlongline, 0, 3)
    # layout.setColumnMinimumWidth(5, 2)
    
    panel.button_vline = QPushButton(panel)
    panel.button_vline.setText('vline')
    panel.button_vline.setShortcut('Ctrl+L')
    panel.button_vline.clicked.connect(ex.draw_vline)
    layout.addWidget(panel.button_vline, 0, 3)
    # layout.setColumnMinimumWidth(6, 2)

    
    panel.button_vlongline = QPushButton(panel)
    panel.button_vlongline.setText('vlongline')
    panel.button_vlongline.setShortcut('Ctrl+Shift+L')
    panel.button_vlongline.clicked.connect(ex.draw_vlongline)
    layout.addWidget(panel.button_vlongline, 0, 3)
    # layout.setColumnMinimumWidth(7, 2)

    panel.button_vcustomline = QPushButton(panel)
    panel.button_vcustomline.setText('line_vcustom')
    panel.button_vcustomline.setShortcut('Alt+L')
    panel.button_vcustomline.clicked.connect(ex.draw_vline_custom)
    layout.addWidget(panel.button_vcustomline, 0, 3)
    # layout.setColumnMinimumWidth(8, 2)

    panel.button_hcustomline = QPushButton(panel)
    panel.button_hcustomline.setText('line_hcustom')
    panel.button_hcustomline.setShortcut('Ctrl+Alt+Space')
    panel.button_hcustomline.clicked.connect(ex.draw_hline_custom)
    layout.addWidget(panel.button_hcustomline, 0, 3)
    # layout.setColumnMinimumWidth(9, 2)

    panel.button_del_last_line = QPushButton(panel)
    panel.button_del_last_line.setText('del last_line')
    panel.button_del_last_line.setShortcut('Ctrl+Backspace')
    panel.button_del_last_line.clicked.connect(ex.del_last_line)
    layout.addWidget(panel.button_del_last_line, 0, 3)
    # layout.setColumnMinimumWidth(10, 2)

    #del_last_text
    panel.button_del_last_text = QPushButton(panel)
    panel.button_del_last_text.setText('del last_text')
    panel.button_del_last_text.setShortcut('Ctrl+Shift+Backspace')
    panel.button_del_last_text.clicked.connect(ex.del_last_text)
    layout.addWidget(panel.button_del_last_text, 0, 3)
    layout.setColumnMinimumWidth(3, 2)

    #================================================================

    panel.darkmode = QCheckBox(panel)
    panel.darkmode.setText('Haxxor mode')
    panel.darkmode.setCheckState(pg.Qt.QtCore.Qt.CheckState.Checked)
    panel.darkmode.toggled.connect(dark_mode_toggle)
    layout.addWidget(panel.darkmode, 0, 4)

    return panel



# import sys
# from PyQt5.QtWidgets import (QApplication, QWidget, QPushButton, QLineEdit, QInputDialog)
class MyApp(QWidget):
    def __init__(self):
        super().__init__()
        self.static_lines=[]
        self.static_texts=[]
    #     self.initUI()

    
    def text_input(self):  #sa(text_input_box => text input on mouse_point)
        dlg =  QInputDialog(self)                 
        dlg.setInputMode(QInputDialog.TextInput) 
        dlg.resize(800,100)  
        # global custom_input_text
        text, ok = dlg.getText(self, 'Text Input Dialog', 'Enter text:')
        clipboard.copy(text)                # text를 clipboard에 copy(재작성시 사용할 용도)
        # result = clipboard.paste(text)    # clipboard의 text를 변수에 넣어 활용
        # print(result)
        if ok:
            x_=pos_x
            y_=pos_y
            text = fplt.add_text((x_, y_), text, color='#bb7700')
            self.static_texts.append(text)
                          
    
    def draw_vline_custom(self):  #sa(draw virtical line from input_box => draw virtical line on mouse_point)
        text, ok = QInputDialog.getText(self, 'Draw line Dialog', 'Enter virtical_line size(+/-):')
        if ok:
            x_=pos_x
            y_=pos_y
            y_1=pos_y+float(text.strip())
            fplt.add_line((x_, y_), (x_, y_1), style='-.', color='gray', width=1.5, interactive=True)  #color='#9900ff'
    
    def draw_hline_custom(self):  #sa(draw horizental line from input_box => draw horizencal line on mouse_point)
        text, ok = QInputDialog.getText(self, 'Draw line Dialog', 'Enter horizental_line size(+/-):')
        if ok:
            x_=pos_x
            x_1=pos_x + float(text.strip())
            y_=pos_y
            fplt.add_line((x_, y_), (x_1, y_), style='-.', color='gray', width=1.5, interactive=True)  #color='#9900ff'
            
    def draw_hline(self):  #수평선
        """ 현재 커서위치에서 작은 수평선을 작성(ctrl+space)"""
        x_=pos_x
        y_=pos_y
        x_1=x_-30 if x_-150>0 else 0
        x_2=x_+30 if x_+250<=len(ws.df) else len(ws.df)
        line = fplt.add_line((x_1, y_), (x_2, y_), style='-.', color='gray', width=1.5, interactive=False)  #color='#9900ff'
        self.static_lines.append(line)
    
    def draw_hlongline(self):  #수평선(전체크기)
        """ 현재 커서위치에서 전체크기의 수평선을 작성(ctrl+shift+space)"""
        x_=pos_x
        y_=pos_y
        x_1=0
        x_2=len(ws.df)
        line = fplt.add_line((x_1, y_), (x_2, y_), style='-.', color='gray', width=1.5, interactive=False)  #color='#9900ff'
        self.static_lines.append(line)
    
    def draw_vline(self):  #수직선
        """ 현재 커서위치에서 작은 수직선을 작성(ctrl+l)"""
        #전체 화면비율 고려 최대치와 최소치 차이 판단
        diff_x=abs(ws.df.Close.max()-ws.df.Close.min())/10
        x_=pos_x
        y_=pos_y
        y_1=y_-diff_x
        y_2=y_+diff_x
        line = fplt.add_line((x_, y_1), (x_, y_2), style='-.', color='gray', width=1.5, interactive=False)  #color='#9900ff'
        self.static_lines.append(line)
    
    def draw_vlongline(self):  #수평선(전체크기)
        """ 현재 커서위치에서 전체크기의 수직선을 작성(ctrl+shift+l)"""
        diff_x=abs(ws.df.Close.max()-ws.df.Close.min())/5
        
        x_=pos_x
        y_=pos_y
        y_1=y_-diff_x
        y_2=y_+diff_x
        line = fplt.add_line((x_, y_1), (x_, y_2), style='-.', color='gray', width=1.5, interactive=False)  #color='#9900ff'
        self.static_lines.append(line)
    
    def del_last_line(self):  # 마지막 정적 라인 삭제
        """ 마지막 고정라인 한개 삭제 (Ctrl + Backspace)"""
        if len(self.static_lines):  # 고정 라인이 한 개라도 있으면 삭제
            fplt.remove_primitive(self.static_lines[-1])
            del self.static_lines[-1]
    
    def del_last_text(self):  # 마지막 정적 라인 삭제
        """ 마지막 고정text 한개 삭제 (Ctrl + shift + Backspace)"""
        if len(self.static_texts):  # 고정 text가 한 개라도 있으면 삭제
            fplt.remove_primitive(self.static_texts[-1])
            del self.static_texts[-1]

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = MyApp()  #위젯 설정


    plots = {}
    fplt.y_pad = 0.07 # pad some extra (for control panel)
    fplt.max_zoom_points = 7
    fplt.autoviewrestore()
    ax,ax_rsi = fplt.create_plot('Complicated Binance Futures Example', rows=2, init_zoom_periods=300)
    axo = ax.overlay()

    # use websocket for real-time
    ws = BinanceFutureWebsocket()

    # hide rsi chart to begin with; show x-axis of top plot
    ax_rsi.hide()
    ax_rsi.vb.setBackgroundColor(None) # don't use odd background color
    ax.set_visible(xaxis=True)

    ctrl_panel = create_ctrl_panel(ax.vb.win)

    # dark_mode 재확인
    dark_mode_toggle(True)
    change_asset()
    fplt.add_crosshair_info(update_crosshair_text, ax=ax) #sa
    fplt.timer_callback(realtime_update_plot, 1) # update every second
    fplt.show()