import pandas as pd
import numpy as np
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sqlite3
import json

# ==============================
# 🔧 【可控制的參數設定】
# ==============================
# 資料夾路徑
FOLDER_PATH = "stock_data"
OUTPUT_CHARTS_FOLDER = "output_charts"

# 資料庫路徑
DB_TSE_PATH = "stock_data/stock_tse_all.db"  # 上市股票資料庫
DB_OTC_PATH = "stock_data/stock_otc_all.db"  # 上櫃股票資料庫

# ==============================
# 篩選條件參數（對應 screen_stocks 函數）
# ==============================
# 條件開關
USE_PRICE = True       # 是否過濾股價上限
USE_MA = True          # 是否要求多頭排列 (趨勢)
USE_VOL = True         # 是否要求量能爆發 (動能)
USE_MIN_VOL = True     # 是否要求最低成交量
USE_INST = True        # 是否要求法人買超 (籌碼)
USE_SHAPE = True       # 是否過濾K線型態 (上影線)

# 變數控制
MAX_PRICE = 100.0           # 股價上限
VOL_RATIO_LIMIT = 1.2       # 成交量倍數 (當日/5日均量)
MIN_VOLUME = 5000           # 最低成交量（張）
SHADOW_LIMIT = 0.2          # 上影線佔比上限 (0.2代表不可超過全幅20%)
MA_SHORT = 5                # 短期均線天數
MA_LONG = 20                # 中期均線天數

# 輸出控制
OUTPUT_CSV = False          # 是否輸出CSV檔案

# ==============================
# 📊 資料庫讀取函數
# ==============================
def read_stock_from_db(stock_code):
    """從資料庫讀取指定股票的資料"""
    df = None
    
    # 先從上市資料庫查詢
    if Path(DB_TSE_PATH).exists():
        try:
            conn = sqlite3.connect(DB_TSE_PATH)
            query = f"SELECT * FROM stock_data WHERE 股票代碼 = '{stock_code}' ORDER BY 日期"
            df = pd.read_sql_query(query, conn)
            conn.close()
            if len(df) > 0:
                return df
        except:
            pass
    
    # 如果上市找不到，從上櫃資料庫查詢
    if Path(DB_OTC_PATH).exists():
        try:
            conn = sqlite3.connect(DB_OTC_PATH)
            query = f"SELECT * FROM stock_data WHERE 股票代碼 = '{stock_code}' ORDER BY 日期"
            df = pd.read_sql_query(query, conn)
            conn.close()
            if len(df) > 0:
                return df
        except:
            pass
    
    return None

def get_all_stock_codes():
    """從資料庫獲取所有股票代碼（排除ETF）"""
    codes = set()
    
    if Path(DB_TSE_PATH).exists():
        try:
            conn = sqlite3.connect(DB_TSE_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT 股票代碼 FROM stock_data")
            codes.update([str(row[0]) for row in cursor.fetchall()])
            conn.close()
        except:
            pass
    
    if Path(DB_OTC_PATH).exists():
        try:
            conn = sqlite3.connect(DB_OTC_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT 股票代碼 FROM stock_data")
            codes.update([str(row[0]) for row in cursor.fetchall()])
            conn.close()
        except:
            pass
    
    # 排除ETF（股票代碼以00開頭的）
    filtered_codes = [code for code in codes if not code.startswith('00')]
    
    return sorted(filtered_codes)

# ==============================
# 📈 【唯一分析引擎】screen_stocks
# ==============================
def screen_stocks(df, 
                  # --- 條件開關 (Flags) ---
                  use_price=True,   # 是否過濾股價上限
                  use_ma=True,      # 是否要求多頭排列 (趨勢)
                  use_vol=True,     # 是否要求量能爆發 (動能)
                  use_min_vol=True, # 是否要求最低成交量
                  use_inst=True,    # 是否要求法人買超 (籌碼)
                  use_shape=True,   # 是否過濾K線型態 (上影線)
                  
                  # --- 變數控制 (Variables) ---
                  max_price=100.0,       # 股價上限
                  vol_ratio_limit=1.2,   # 成交量倍數 (當日/5日均量)
                  min_volume=5000,       # 最低成交量（張）
                  shadow_limit=0.2,      # 上影線佔比上限 (0.2代表不可超過全幅20%)
                  ma_short=5,            # 短期均線天數
                  ma_long=20             # 中期均線天數
                  ):
    """
    股票篩選分析引擎（接受 DataFrame）
    
    返回格式：
    {
        "股票": "2330 台積電",
        "收盤價": 580.0,
        "漲跌幅": "1.5%",
        "量能倍數": 1.5,
        "法人買超張數": 1000,
        "上影線比例": "10.0%"
    }
    或 None（不符合條件）
    """
    try:
        # 1. 數據載入與基本計算
        df = df.copy()
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        # 移除千位分隔符並轉換數值
        for col in ['收盤價', '開盤價', '最高價', '最低價', '成交張數', 
                    '外陸資買賣超張數', '投信買賣超張數', '自營商買賣超張數']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if len(df) < ma_long: 
            return None
            
        df['MA_S'] = df['收盤價'].rolling(window=ma_short).mean()
        df['MA_L'] = df['收盤價'].rolling(window=ma_long).mean()
        df['MA20'] = df['收盤價'].rolling(window=20).mean()
        df['MA60'] = df['收盤價'].rolling(window=60).mean()
        df['VolMA'] = df['成交張數'].rolling(window=ma_short).mean()
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 2. 條件判斷邏輯 (若 Flag 為 False，則該條件恆為 True)
        
        # 價格條件
        c_price = (latest['收盤價'] <= max_price) if use_price else True
        
        # 均線趨勢 (收盤 > 短均 > 長均)
        c_ma = (latest['收盤價'] > latest['MA_S'] > latest['MA_L']) if use_ma else True
        
        # 量能爆發
        actual_vol_ratio = latest['成交張數'] / latest['VolMA'] if latest['VolMA'] != 0 else 0
        c_vol = (actual_vol_ratio >= vol_ratio_limit) if use_vol else True
        
        # 最低成交量
        c_min_vol = (latest['成交張數'] >= min_volume) if use_min_vol else True
        
        # 法人籌碼 (外資+投信+自營商合計買超)
        inst_total = latest['外陸資買賣超張數'] + latest['投信買賣超張數'] + latest['自營商買賣超張數']
        c_inst = (inst_total > 0) if use_inst else True
        
        # K線型態 (避免追高受阻留長上影線)
        candle_range = latest['最高價'] - latest['最低價']
        upper_shadow = latest['最高價'] - max(latest['開盤價'], latest['收盤價'])
        actual_shadow_ratio = upper_shadow / (candle_range + 0.01)
        c_shape = (actual_shadow_ratio <= shadow_limit) if use_shape else True
        
        # 3. 綜合判定
        if all([c_price, c_ma, c_vol, c_min_vol, c_inst, c_shape]):
            return {
                "股票": f"{latest['股票代碼']} {latest['股票名稱']}",
                "收盤價": latest['收盤價'],
                "漲跌幅": f"{round(((latest['收盤價']-prev['收盤價'])/prev['收盤價'])*100, 2)}%",
                "量能倍數": round(actual_vol_ratio, 2),
                "法人買超張數": inst_total,
                "上影線比例": f"{round(actual_shadow_ratio*100, 1)}%",
                "latest_date": latest['日期'].strftime('%Y.%m.%d'),
                "stock_code": latest['股票代碼'],
                "stock_name": latest['股票名稱']
            }
        return None
        
    except Exception as e:
        print(f"處理股票時出錯: {e}")
        return None

# ==============================
# 📚 載入公司資訊
# ==============================
def load_company_lists():
    """
    載入公司資訊 (上市/上櫃)
    回傳格式: {
        'code': {'name': '公司名稱', 'type': '上市/上櫃', 'sector': '產業分類'}
    }
    """
    company_info = {}
    
    # 讀取上市公司
    tse_file = Path(FOLDER_PATH) / "公司代號及名稱(上市).txt"
    if tse_file.exists():
        try:
            with open(tse_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            for line in lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    code = parts[0].strip()
                    name = parts[1].strip()
                    sector = parts[2].strip()
                    company_info[code] = {
                        'name': name,
                        'type': '上市',
                        'sector': sector
                    }
            print(f"✅ 讀取上市公司清單: {len([k for k, v in company_info.items() if v['type'] == '上市'])} 家")
        except Exception as e:
            print(f"⚠️ 讀取上市公司清單失敗: {e}")
    else:
        print(f"⚠️ 找不到檔案: {tse_file}")
    
    # 讀取上櫃公司
    otc_file = Path(FOLDER_PATH) / "公司代號及名稱(上櫃).txt"
    if otc_file.exists():
        try:
            with open(otc_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            for line in lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    code = parts[0].strip()
                    name = parts[1].strip()
                    sector = parts[2].strip()
                    company_info[code] = {
                        'name': name,
                        'type': '上櫃',
                        'sector': sector
                    }
            print(f"✅ 讀取上櫃公司清單: {len([k for k, v in company_info.items() if v['type'] == '上櫃'])} 家")
        except Exception as e:
            print(f"⚠️ 讀取上櫃公司清單失敗: {e}")
    else:
        print(f"⚠️ 找不到檔案: {otc_file}")
    
    return company_info

# ==============================
# 📈 生成單檔股票圖表
# ==============================
def generate_stock_chart(stock_code, stock_name, csv_file, output_folder, stock_type='未知', stock_sector='未知', industry_category=None):
    """生成單檔股票的HTML圖表，先分析後命名"""
    try:
        # 從資料庫讀取資料
        df = read_stock_from_db(stock_code)
        if df is None or len(df) == 0:
            print(f"        ⚠️ 無法從資料庫讀取 {stock_code} {stock_name} 的資料")
            return False
        
        # 轉換資料類型
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 移除千位分隔符逗號後再轉換數值
        for col in ['開盤價', '最高價', '最低價', '收盤價', '成交張數',
                    '外陸資買賣超張數', '投信買賣超張數', '自營商買賣超張數']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.dropna(subset=['日期'], inplace=True)
        df.sort_values('日期', inplace=True)
        
        # 取得最後一天收盤價
        latest_close = df['收盤價'].iloc[-1]
        latest_close_str = f"{latest_close:.2f}"
        
        # ===== 執行篩選分析（用新的 screen_stocks 引擎）=====
        screen_result = screen_stocks(
            df,
            use_price=USE_PRICE,
            use_ma=USE_MA,
            use_vol=USE_VOL,
            use_inst=USE_INST,
            use_shape=USE_SHAPE,
            max_price=MAX_PRICE,
            vol_ratio_limit=VOL_RATIO_LIMIT,
            shadow_limit=SHADOW_LIMIT,
            ma_short=MA_SHORT,
            ma_long=MA_LONG
        )
        
        # 轉換成原本 analyze_volume_price_pattern 的格式
        if screen_result:
            analysis = {
                'action': '上車',
                'risk_level': '中',
                'score': 5,
                'summary': f"符合篩選條件：量能倍數 {screen_result['量能倍數']}，法人買超 {screen_result['法人買超張數']:.0f}張",
                'signals': [
                    f"✅ 收盤價: {screen_result['收盤價']:.2f}",
                    f"📊 漲跌幅: {screen_result['漲跌幅']}",
                    f"🔥 量能倍數: {screen_result['量能倍數']}",
                    f"💰 法人買超: {screen_result['法人買超張數']:.0f}張",
                    f"📈 上影線比例: {screen_result['上影線比例']}"
                ]
            }
        else:
            analysis = {
                'action': '觀望',
                'risk_level': '中',
                'score': 0,
                'summary': '不符合篩選條件',
                'signals': []
            }
        
        # 根據操作建議決定檔案名稱（加入收盤價）
        action = analysis['action']
        
        # 根據是否為概念股模式決定檔名格式
        if industry_category:
            # 概念股模式：產業分類_股票代號_股票名稱_最新收盤價.html
            output_filename = f"{industry_category}_{stock_code}_{stock_name}_{latest_close_str}.html"
        else:
            # 一般模式：股票代號_股票名稱_最新收盤價.html
            output_filename = f"{stock_code}_{stock_name}_{latest_close_str}.html"
        
        output_path = output_folder / output_filename
        
        # 取最近60筆資料
        df_chart = df.tail(60).copy()
        
        # 計算移動平均線
        df_chart['MA5'] = df_chart['收盤價'].rolling(window=5, min_periods=1).mean()
        df_chart['MA10'] = df_chart['收盤價'].rolling(window=10, min_periods=1).mean()
        df_chart['MA20'] = df_chart['收盤價'].rolling(window=20, min_periods=1).mean()
        df_chart['MA60'] = df_chart['收盤價'].rolling(window=60, min_periods=1).mean()
        
        # 創建子圖
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            subplot_titles=('', '', '', ''),
            row_heights=[0.4, 0.2, 0.2, 0.2],
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}]]
        )
        
        # 第一層：K線圖
        fig.add_trace(
            go.Candlestick(
                x=df_chart['日期'],
                open=df_chart['開盤價'],
                high=df_chart['最高價'],
                low=df_chart['最低價'],
                close=df_chart['收盤價'],
                name='K線',
                increasing_line_color='#FF5252',
                increasing_fillcolor='#FF5252',
                decreasing_line_color='#00C851',
                decreasing_fillcolor='#00C851',
                line=dict(width=0.8),
            ),
            row=1, col=1
        )
        
        # 添加MA5、MA10、MA20、MA60
        for ma_name, ma_col, color in [
            ('MA5', 'MA5', 'blue'), 
            ('MA10', 'MA10', 'orange'),
            ('MA20', 'MA20', 'green'),
            ('MA60', 'MA60', 'purple')
        ]:
            if ma_col in df_chart.columns and df_chart[ma_col].notna().sum() > 0:
                fig.add_trace(
                    go.Scatter(
                        x=df_chart['日期'],
                        y=df_chart[ma_col],
                        name=ma_name,
                        line=dict(color=color, width=1.5),
                        mode='lines',
                    ),
                    row=1, col=1
                )
        
        # 第二層：成交量
        if '成交張數' in df_chart.columns:
            volume_lots = pd.to_numeric(df_chart['成交張數'], errors='coerce')
            colors = []
            for i in range(len(df_chart)):
                if i == 0:
                    if df_chart['收盤價'].iloc[i] >= df_chart['開盤價'].iloc[i]:
                        colors.append('rgba(255, 82, 82, 0.8)')
                    else:
                        colors.append('rgba(0, 200, 81, 0.8)')
                else:
                    if df_chart['收盤價'].iloc[i] >= df_chart['收盤價'].iloc[i-1]:
                        colors.append('rgba(255, 82, 82, 0.8)')
                    else:
                        colors.append('rgba(0, 200, 81, 0.8)')
            
            fig.add_trace(
                go.Bar(
                    x=df_chart['日期'],
                    y=volume_lots,
                    name='成交量',
                    marker=dict(color=colors, line=dict(width=0)),
                    showlegend=True
                ),
                row=2, col=1
            )
        
        # 第三層：三大法人當日買賣超
        has_institutional = False
        if '外陸資買賣超張數' in df_chart.columns:
            foreign = pd.to_numeric(df_chart['外陸資買賣超張數'], errors='coerce')
            trust = pd.to_numeric(df_chart.get('投信買賣超張數', 0), errors='coerce')
            dealer = pd.to_numeric(df_chart.get('自營商買賣超張數', 0), errors='coerce')
            
            if foreign.notna().sum() > 0 or trust.notna().sum() > 0 or dealer.notna().sum() > 0:
                has_institutional = True
                for name, data, color in [
                    ('外資', foreign, 'rgba(255, 82, 82, 0.75)'),
                    ('投信', trust, 'rgba(0, 200, 81, 0.75)'),
                    ('自營商', dealer, 'rgba(0, 191, 255, 0.75)')
                ]:
                    fig.add_trace(
                        go.Bar(
                            x=df_chart['日期'],
                            y=data,
                            name=name,
                            marker_color=color,
                            legendgroup=name,
                            showlegend=True
                        ),
                        row=3, col=1
                    )
        
        # 第四層：三大法人累積買賣超
        if has_institutional:
            foreign_cumsum = pd.to_numeric(df_chart['外陸資買賣超張數'], errors='coerce').fillna(0).cumsum()
            trust_cumsum = pd.to_numeric(df_chart.get('投信買賣超張數', 0), errors='coerce').fillna(0).cumsum()
            dealer_cumsum = pd.to_numeric(df_chart.get('自營商買賣超張數', 0), errors='coerce').fillna(0).cumsum()
            
            for name, data, color in [
                ('外資', foreign_cumsum, 'rgb(255, 82, 82)'),
                ('投信', trust_cumsum, 'rgb(0, 200, 81)'),
                ('自營商', dealer_cumsum, 'rgb(0, 191, 255)')
            ]:
                fig.add_trace(
                    go.Scatter(
                        x=df_chart['日期'],
                        y=data,
                        name=f'{name}累積',
                        line=dict(color=color, width=2.5, shape='spline', smoothing=0.8),
                        mode='lines',
                        legendgroup=name,
                        showlegend=True
                    ),
                    row=4, col=1
                )
        
        # 計算統計數據
        latest = df_chart.iloc[-1]
        latest_date_str = latest['日期'].strftime('%Y.%m.%d')
        stats = {
            '成交量': latest['成交張數'] if '成交張數' in latest and pd.notna(latest['成交張數']) else 0,
            '外資累積': foreign_cumsum.iloc[-1] if has_institutional and len(foreign_cumsum) > 0 else 0,
            '投信累積': trust_cumsum.iloc[-1] if has_institutional and len(trust_cumsum) > 0 else 0,
            '自營累積': dealer_cumsum.iloc[-1] if has_institutional and len(dealer_cumsum) > 0 else 0,
        }
        
        # 更新佈局
        stats_line1 = (
            f"最新資料日期: {latest_date_str} | "
            f"外資累積: {stats['外資累積']:,.0f}張 | "
            f"投信累積: {stats['投信累積']:,.0f}張 | "
            f"自營累積: {stats['自營累積']:,.0f}張"
        )
        stats_line2 = f"股價K線圖 | 成交量: {stats['成交量']:,.0f}張"
        
        fig.update_layout(
            title=dict(
                text=f'{stock_code} {stock_name} ({stock_type} | {stock_sector}) 技術分析圖表 (最近60筆)<br><sub>{stats_line1}</sub><br><sub>{stats_line2}</sub>',
                x=0.5,
                xanchor='center',
                font=dict(size=16, family='Microsoft JhengHei, Arial, sans-serif')
            ),
            xaxis_rangeslider_visible=False,
            height=1500,
            showlegend=True,
            hovermode='x unified',
            template='plotly_white',
            barmode='relative',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=0.98,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="lightgray",
                borderwidth=1,
                font=dict(family='Microsoft JhengHei, Arial, sans-serif')
            ),
            font=dict(family='Microsoft JhengHei, Arial, sans-serif'),
            dragmode='pan'
        )
        
        # 更新Y軸
        price_cols = ['開盤價', '最高價', '最低價', '收盤價']
        price_min = df_chart[price_cols].min().min()
        price_max = df_chart[price_cols].max().max()
        price_margin = (price_max - price_min) * 0.05
        price_range = [price_min - price_margin, price_max + price_margin]
        
        fig.update_yaxes(title_text="股價 (元)", row=1, col=1, range=price_range, fixedrange=True)
        fig.update_yaxes(title_text="成交量 (張)", row=2, col=1, tickformat=",", fixedrange=True)
        fig.update_yaxes(title_text="當日買賣超 (張)", row=3, col=1, tickformat=",", fixedrange=True)
        fig.update_yaxes(title_text="累積買賣超 (張)", row=4, col=1, tickformat=",", fixedrange=True)
        
        # 更新X軸 - 移除非交易日空隙
        start_date = df_chart['日期'].min()
        end_date = df_chart['日期'].max()
        trading_dates = df_chart['日期'].tolist()
        
        # 生成刻度值（每月1、6、11、16、21、26日）
        tickvals = []
        current = start_date.replace(day=1)
        while current <= end_date:
            for day in [1, 6, 11, 16, 21, 26]:
                try:
                    tick_date = current.replace(day=day)
                    if start_date <= tick_date <= end_date:
                        tickvals.append(tick_date)
                except:
                    pass
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        for i in range(1, 5):
            fig.update_xaxes(
                tickformat="%m-%d",
                tickangle=-45,
                tickmode='array',
                tickvals=tickvals,
                showticklabels=True,
                autorange=True,
                hoverformat="%m-%d",
                fixedrange=True,
                rangebreaks=[
                    dict(values=pd.date_range(start=start_date, end=end_date, freq='D')
                         .difference(pd.DatetimeIndex(trading_dates)).tolist())
                ],
                row=i, col=1
            )
        
        # 生成HTML
        html_string = fig.to_html(include_plotlyjs='cdn')
        
        # 生成分析區塊的HTML
        # 根據操作建議選擇顏色
        action_colors = {
            '重倉': '#FF4444',
            '上車': '#00C851',
            '觀望': '#FFA500',
            '減倉': '#FF8800',
            '清倉': '#CC0000'
        }
        action_color = action_colors.get(analysis['action'], '#666666')
        
        # 根據風險等級選擇顏色
        risk_colors = {
            '低': '#00C851',
            '中': '#FFA500',
            '高': '#FF4444'
        }
        risk_color = risk_colors.get(analysis['risk_level'], '#666666')
        
        # 生成信號列表HTML
        signals_html = ""
        if analysis['signals']:
            signals_html = "<ul style='margin: 10px 0; padding-left: 25px; line-height: 1.8;'>"
            for signal in analysis['signals']:
                signals_html += f"<li style='margin: 5px 0;'>{signal}</li>"
            signals_html += "</ul>"
        else:
            signals_html = "<p style='color: #999; font-style: italic;'>暫無明確信號</p>"
        
        # 評分進度條
        score = analysis['score']
        # 將評分映射到 0-100 的進度條（-10到10映射到0-100）
        progress = min(100, max(0, (score + 10) * 5))
        
        # 根據評分選擇進度條顏色
        if score >= 5:
            progress_color = '#00C851'  # 綠色
        elif score >= 0:
            progress_color = '#FFA500'  # 橙色
        elif score >= -5:
            progress_color = '#FF8800'  # 深橙
        else:
            progress_color = '#FF4444'  # 紅色
        
        analysis_block = f'''
<div style="max-width: 1200px; margin: 30px auto; padding: 20px; font-family: 'Microsoft JhengHei', Arial, sans-serif;">
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px 10px 0 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <h2 style="margin: 0; font-size: 24px; display: flex; align-items: center;">
            <span style="font-size: 30px; margin-right: 10px;">📊</span>
            量價戰法分析
        </h2>
        <p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.9;">基於量價關係、K線型態、趨勢判斷的綜合分析</p>
    </div>
    
    <div style="background: white; padding: 25px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 10px 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <!-- 核心指標卡片 -->
        <div style="display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap;">
            <!-- 操作建議卡 -->
            <div style="flex: 1; min-width: 200px; background: linear-gradient(135deg, {action_color}15, {action_color}25); border-left: 4px solid {action_color}; padding: 15px; border-radius: 8px;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">💡 操作建議</div>
                <div style="font-size: 28px; font-weight: bold; color: {action_color};">{analysis['action']}</div>
            </div>
            
            <!-- 風險等級卡 -->
            <div style="flex: 1; min-width: 200px; background: linear-gradient(135deg, {risk_color}15, {risk_color}25); border-left: 4px solid {risk_color}; padding: 15px; border-radius: 8px;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">⚠️ 風險等級</div>
                <div style="font-size: 28px; font-weight: bold; color: {risk_color};">{analysis['risk_level']}</div>
            </div>
            
            <!-- 評分卡 -->
            <div style="flex: 1; min-width: 200px; background: linear-gradient(135deg, {progress_color}15, {progress_color}25); border-left: 4px solid {progress_color}; padding: 15px; border-radius: 8px;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">📈 綜合評分</div>
                <div style="font-size: 28px; font-weight: bold; color: {progress_color};">{score} 分</div>
                <div style="background: #e0e0e0; height: 8px; border-radius: 4px; margin-top: 8px; overflow: hidden;">
                    <div style="background: {progress_color}; height: 100%; width: {progress}%; transition: width 0.3s ease;"></div>
                </div>
            </div>
        </div>
        
        <!-- 信號列表 -->
        <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; border: 1px solid #e9ecef;">
            <h3 style="margin: 0 0 15px 0; font-size: 18px; color: #333; display: flex; align-items: center;">
                <span style="font-size: 22px; margin-right: 8px;">🔍</span>
                技術信號分析
            </h3>
            {signals_html}
        </div>
        
        <!-- 評分說明 -->
        <div style="margin-top: 20px; padding: 15px; background: #fff3cd; border-left: 4px solid #ffc107; border-radius: 4px;">
            <div style="font-size: 14px; color: #856404; line-height: 1.6;">
                <strong>📖 評分標準：</strong>
                <span style="display: inline-block; margin: 0 10px;">≥8分=重倉</span>
                <span style="display: inline-block; margin: 0 10px;">5-7分=上車</span>
                <span style="display: inline-block; margin: 0 10px;">-4~4分=觀望</span>
                <span style="display: inline-block; margin: 0 10px;">-5~-7分=減倉</span>
                <span style="display: inline-block; margin: 0 10px;">≤-8分=清倉</span>
            </div>
        </div>
        
        <!-- 免責聲明 -->
        <div style="margin-top: 20px; padding: 12px; background: #f8f9fa; border-radius: 4px; font-size: 12px; color: #6c757d; text-align: center;">
            ⚠️ 本分析僅供參考，不構成投資建議。股市有風險，投資需謹慎。
        </div>
    </div>
</div>
'''
        
        # 包裝完整HTML
        viewport_meta = '<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, minimum-scale=1.0, user-scalable=no">'
        full_html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    {viewport_meta}
    <title>{action} - {stock_code} {stock_name}</title>
    <style>
        body {{ margin: 0; padding: 0; background: #f5f5f5; }}
    </style>
</head>
<body>
{html_string}
{analysis_block}
</body>
</html>'''
        
        # 儲存檔案
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(full_html)
        
        print(f"  ✓ 圖表已生成: {output_path}")
        
        # 在終端也輸出分析
        print(f"  📊 走勢分析:")
        print(f"     操作建議: {analysis['action']} | 風險等級: {analysis['risk_level']} | 評分: {analysis['score']}")
        
        if analysis['signals']:
            print(f"     信號列表:")
            for signal in analysis['signals']:
                print(f"       • {signal}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ 生成圖表失敗: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==============================
# 💾 保存到 stock_hot.db
# ==============================
def save_to_hot_db(results, company_info, latest_date_str, focus_stock_codes=None, is_first_stage=True):
    """將符合條件的股票完整交易歷史保存到 stock_hot.db
    
    參數:
        focus_stock_codes: focus_stocks.csv 中的股票代碼集合
        is_first_stage: True=第一階段（會刪除舊資料庫），False=第二階段（追加資料）
    """
    try:
        db_path = "stock_data/stock_hot.db"
        
        # 第一階段：刪除舊資料庫，創建全新資料庫
        if is_first_stage:
            if Path(db_path).exists():
                Path(db_path).unlink()
                print(f"🗑️  已刪除舊資料庫")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 創建表格 - 包含完整的 OHLCV 資料
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hot_stocks (
                股票代碼 TEXT,
                股票名稱 TEXT,
                類型 TEXT,
                產業分類 TEXT,
                日期 TEXT,
                開盤價 REAL,
                最高價 REAL,
                最低價 REAL,
                收盤價 REAL,
                成交量 INTEGER,
                成交筆數 TEXT,
                成交金額 TEXT,
                本益比 TEXT,
                外陸資買賣超張數 REAL,
                投信買賣超張數 REAL,
                自營商買賣超張數 REAL,
                更新時間 TEXT,
                IS_FOCUS INTEGER,
                PRIMARY KEY (股票代碼, 日期)
            )
        ''')
        
        from datetime import datetime
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        total_records = 0
        for r in results:
            code = r['code']
            info = company_info.get(code, {})
            name = info.get('name', '未知')
            type_str = info.get('type', '未知')
            sector = info.get('sector', '未知')
            
            # 讀取該股票的完整歷史資料
            stock_df = read_stock_from_db(code)
            if stock_df is None or len(stock_df) == 0:
                continue
            
            # 確保數據類型正確
            stock_df = stock_df.copy()
            for col in ['開盤價', '最高價', '最低價', '收盤價', '成交張數']:
                if col in stock_df.columns:
                    stock_df[col] = stock_df[col].astype(str).str.replace(',', '', regex=False)
                    stock_df[col] = pd.to_numeric(stock_df[col], errors='coerce')
            
            # 將每一天的資料都寫入資料庫
            for idx, row in stock_df.iterrows():
                # 轉換日期格式為統一格式 YYYY.MM.DD
                try:
                    date_obj = pd.to_datetime(row['日期'])
                    date_str = date_obj.strftime('%Y.%m.%d')
                except:
                    date_str = str(row['日期'])
                
                # 判斷是否為 focus 股票
                is_focus = 1 if (focus_stock_codes and code in focus_stock_codes) else 0
                
                cursor.execute('''
                    INSERT OR REPLACE INTO hot_stocks 
                    (股票代碼, 股票名稱, 類型, 產業分類, 日期, 
                     開盤價, 最高價, 最低價, 收盤價, 成交量,
                     成交筆數, 成交金額, 本益比,
                     外陸資買賣超張數, 投信買賣超張數, 自營商買賣超張數,
                     更新時間, IS_FOCUS)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code, name, type_str, sector, date_str,
                    float(row.get('開盤價', 0)) if not pd.isna(row.get('開盤價')) else 0,
                    float(row.get('最高價', 0)) if not pd.isna(row.get('最高價')) else 0,
                    float(row.get('最低價', 0)) if not pd.isna(row.get('最低價')) else 0,
                    float(row.get('收盤價', 0)) if not pd.isna(row.get('收盤價')) else 0,
                    int(row.get('成交張數', 0)) if not pd.isna(row.get('成交張數')) else 0,
                    str(row.get('成交筆數', '')),
                    str(row.get('成交金額', '')),
                    str(row.get('本益比', '')),
                    float(row.get('外陸資買賣超張數', 0)) if not pd.isna(row.get('外陸資買賣超張數')) else 0,
                    float(row.get('投信買賣超張數', 0)) if not pd.isna(row.get('投信買賣超張數')) else 0,
                    float(row.get('自營商買賣超張數', 0)) if not pd.isna(row.get('自營商買賣超張數')) else 0,
                    update_time, is_focus
                ))
                total_records += 1
        
        conn.commit()
        conn.close()
        
        stage_name = "第一階段" if is_first_stage else "第二階段"
        print(f"✅ {stage_name}：已將 {len(results)} 檔股票的 {total_records} 筆歷史資料保存到 stock_hot.db")
        
    except Exception as e:
        print(f"❌ 保存到資料庫失敗: {e}")
        import traceback
        traceback.print_exc()

# ==============================
# 🚀 主程式
# ==============================
def main():
    """
    主程式：兩階段分析
    第一階段：一般模式（全股掃描）
    第二階段：追蹤清單模式（focus_stocks.csv）
    """
    # 載入公司資訊
    company_info = load_company_lists()
    print(f"📋 已載入 {len(company_info)} 家公司資訊")
    
    # 除錯：顯示前5筆公司資訊
    if len(company_info) > 0:
        sample_codes = list(company_info.keys())[:5]
        print("   範例資料:")
        for code in sample_codes:
            info = company_info[code]
            print(f"   {code}: {info['name']} ({info['type']} | {info['sector']})")
    else:
        print("⚠️ 警告：未能載入任何公司資訊！")
    print()
    
    # 取得資料庫中最新的日期
    latest_date_str = None
    try:
        if Path(DB_TSE_PATH).exists():
            conn = sqlite3.connect(DB_TSE_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(日期) FROM stock_data")
            result = cursor.fetchone()
            if result and result[0]:
                latest_date_str = pd.to_datetime(result[0]).strftime('%Y.%m.%d')
            conn.close()
    except:
        pass
    
    if not latest_date_str:
        print("⚠️ 無法取得最新日期，使用今日日期")
        from datetime import datetime
        latest_date_str = datetime.now().strftime('%Y.%m.%d')
    
    print(f"📅 最新資料日期: {latest_date_str}\n")
    
    # 建立輸出資料夾
    base_output_folder = Path(OUTPUT_CHARTS_FOLDER)
    base_output_folder.mkdir(exist_ok=True)
    
    # 建立以日期命名的子資料夾（前綴 full_）
    output_folder = base_output_folder / f"full_{latest_date_str}_Gemini"
    output_folder.mkdir(exist_ok=True)
    
    # ==========================================
    # 全市場掃描模式
    # ==========================================
    print("=" * 70)
    print("🔍 全市場掃描模式")
    print("=" * 70)
    
    stock_codes = get_all_stock_codes()
    if not stock_codes:
        print(f"📁 資料庫中沒有股票資料！")
        return
    
    print(f"📁 輸出資料夾: {output_folder}")
    print(f"📊 股票總數: {len(stock_codes)} 檔（已排除ETF）\n")

    enabled = []
    if USE_PRICE: enabled.append(f"股價≤{MAX_PRICE}")
    if USE_MA: enabled.append("多頭排列")
    if USE_VOL: enabled.append(f"量能≥{VOL_RATIO_LIMIT}倍")
    if USE_MIN_VOL: enabled.append(f"成交量≥{MIN_VOLUME}張")
    if USE_INST: enabled.append("法人買超")
    if USE_SHAPE: enabled.append(f"上影線≤{SHADOW_LIMIT*100}%")
    
    print(f"🔍 掃描 {len(stock_codes)} 檔股票...")
    print(f"   • 啟用條件: {' + '.join(enabled) if enabled else '無'}")
    print(f"   • 輸出CSV: {'✅ 啟用' if OUTPUT_CSV else '❌ 關閉'}")
    print()

    # 篩選符合條件的股票
    results = []
    for stock_code in stock_codes:
        df = read_stock_from_db(stock_code)
        if df is None or len(df) == 0:
            continue
        
        res = screen_stocks(
            df,
            use_price=USE_PRICE,
            use_ma=USE_MA,
            use_vol=USE_VOL,
            use_min_vol=USE_MIN_VOL,
            use_inst=USE_INST,
            use_shape=USE_SHAPE,
            max_price=MAX_PRICE,
            vol_ratio_limit=VOL_RATIO_LIMIT,
            min_volume=MIN_VOLUME,
            shadow_limit=SHADOW_LIMIT,
            ma_short=MA_SHORT,
            ma_long=MA_LONG
        )
        
        if res:
            results.append({
                'code': res['stock_code'],
                'latest_date': res['latest_date'],
                'latest_close': res['收盤價'],
                'last_volume': df['成交張數'].iloc[-1] if '成交張數' in df.columns else 0
            })

    # 按成交量排序
    results.sort(key=lambda x: x.get('last_volume', 0), reverse=True)

    print("=" * 70)
    
    if results:
        print(f"✅ 找到 {len(results)} 檔符合基本條件，將進一步篩選「上車」建議：\n")
        
        chart_count = 0
        for r in results:
            code = r['code']
            
            # 從 company_info 取得股票資訊
            info = company_info.get(code, {})
            name = info.get('name', '未知')
            type_str = info.get('type', '未知')
            sector = info.get('sector', '未知')
            
            # 除錯：如果是「未知」，嘗試從資料庫中讀取
            if name == '未知':
                print(f"    ⚠️ 在 company_info 中找不到 {code} 的資訊")
                stock_df_temp = read_stock_from_db(code)
                if stock_df_temp is not None and len(stock_df_temp) > 0:
                    if '股票名稱' in stock_df_temp.columns:
                        name = stock_df_temp['股票名稱'].iloc[0]
                        print(f"    ✓ 從資料庫中找到名稱: {name}")
                    else:
                        print(f"    ⚠️ 資料庫中也沒有「股票名稱」欄位")
                        # 顯示資料庫的所有欄位
                        print(f"    資料庫欄位: {list(stock_df_temp.columns)}")

            print(f"{code} | {name} | {type_str} | {sector} | 日期: {r['latest_date']} | 收盤: {r['latest_close']:.2f}")
            
            # 讀取股票資料
            stock_df = read_stock_from_db(code)
            if stock_df is not None and len(stock_df) >= 10:
                # 生成圖表
                print(f"    🎨 生成圖表...")
                if generate_stock_chart(code, name, None, output_folder, type_str, sector):
                    chart_count += 1
                
                # 輸出 CSV 檔案（依據Flag控制）
                if OUTPUT_CSV:
                    try:
                        csv_path = output_folder / f"{code}_{name}.csv"
                        stock_df_sorted = stock_df.sort_values('日期', ascending=False)
                        stock_df_sorted.to_csv(csv_path, index=False, encoding='utf-8-sig')
                        print(f"    📄 輸出 CSV: {code}_{name}.csv")
                    except Exception as e:
                        print(f"    ⚠️  輸出 CSV 失敗: {e}")
            else:
                print(f"    ⚠️  資料不足，無法分析")
            
            print()
        
        # 保存到資料庫
        save_to_hot_db(results, company_info, latest_date_str, set(), is_first_stage=True)
        
        print("=" * 70)
        print(f"✅ 掃描完成：成功生成 {chart_count} 個圖表")
        print(f"   • 輸出資料夾: {output_folder}")
    else:
        print("❌ 未找到符合所有啟用條件的股票")

if __name__ == "__main__":
    main()
