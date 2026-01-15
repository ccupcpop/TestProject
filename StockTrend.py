import os
import pandas as pd
import numpy as np
from pathlib import Path
import re

# ====== 策略參數（可依需求調整）======

MIN_DATA_DAYS = 60              #60 至少需要多少天的歷史資料才進行分析（避免新上市股票資料不足）
RECENT_BOTTOM_WINDOW = 10       #15 法人買賣超累積淨額的「最低點」必須出現在最近 N 天內（確保是近期深底）
MIN_REBOUND_AMOUNT = 500        #1000 從上述最低點反彈的張數至少要超過 N 張（代表法人開始明顯回補）
RISING_CHECK_DAYS = 5           #5 在最近 N 天內，法人累積買賣超淨額不能下跌（需連續持平或上升）
PRICE_RECENT_LOW_WINDOW = 10    #10 股價的 60 日最低點必須出現在最近 N 天內（確認股價剛創階段新低）
PRICE_RISING_DAYS = 2           #3 股價在最近 N 天必須連續上漲（代表止跌反彈已啟動）
RECENT_OSCILLATION_DAYS = 15    #15 檢查最近 N 天三大法人買賣超是否有明顯震盪（用來識別「震盪洗盤」）
OSCILLATION_MIN_RANGE = 500     #500 震盪幅度門檻：最近 N 天內最大單日買超與最小單日賣超之差需大於 N 張（確保有真實多空拉鋸）

# ===================================

# ================================
# 🔍 量價形態分析函式
# ================================
def analyze_volume_price_pattern(df):
    """
    基於最新兩筆資料，判斷量價形態。
    回傳 (pattern_name, interpretation)
    """
    if len(df) < 2:
        return "資料不足", "無法判斷量價形態"

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    vol_latest = latest['volume']
    vol_prev = prev['volume']
    price_latest = latest['close']
    price_prev = prev['close']

    if pd.isna(vol_latest) or pd.isna(vol_prev) or pd.isna(price_latest) or pd.isna(price_prev):
        return "資料無效", "價格或成交量缺失"

    # 判斷成交量變化（±10% 閾值）
    if vol_latest > vol_prev * 1.1:
        vol_trend = "增"
    elif vol_latest < vol_prev * 0.9:
        vol_trend = "縮"
    else:
        vol_trend = "平"

    # 判斷價格變化（±1% 閾值）
    if price_latest > price_prev * 1.01:
        price_trend = "漲"
    elif price_latest < price_prev * 0.99:
        price_trend = "跌"
    else:
        price_trend = "平"

    pattern = f"量{vol_trend}價{price_trend}"

    interpretations = {
        "量增價漲": "積極信號：上漲動能強勁，可持續關注。",
        "量增價平": "多空博弈：主力吸籌或試盤，觀察突破方向。",
        "量增價跌": "主力出貨：拋壓沉重，謹慎看待反彈。",
        "量平價漲": "穩健上漲：惜售氣氛濃厚，但需補量確認。",
        "量平價平": "方向不明：市場觀望，等待催化劑。",
        "量平價跌": "弱勢格局：下跌無量，可能陰跌。",
        "量縮價漲": "謹防回調：上漲動能不足，追高風險高。",
        "量縮價平": "交投清淡：缺乏參與意願，趨勢不明。",
        "量縮價跌": "賣壓減輕：可能接近底部，但尚未止跌。"
    }

    interp = interpretations.get(pattern, "未知形態")
    return pattern, interp


def fix_price_columns(row):
    """修復因缺少逗號而黏在一起的價格欄位"""
    if pd.isna(row['open']):
        return row
    open_val = str(row['open']).strip()
    if len(open_val) > 12 and '.' in open_val:
        prices = re.findall(r'\d+\.\d{1,3}', open_val)
        if len(prices) >= 4:
            row['open'] = prices[0]
            row['high'] = prices[1]
            row['low'] = prices[2]
            row['close'] = prices[3]
        else:
            row['open'] = np.nan
    return row


def analyze_stock_file(file_path):
    cols = [
        'date', 'code', 'name', 'volume', 'trades', 'amount',
        'open', 'high', 'low', 'close', 'pe',
        'foreign_net', 'fund_net', 'dealer_net'
    ]
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            has_header = any(kw in first_line for kw in ['日期', '日 期', '股 票'])
            if has_header:
                df = pd.read_csv(file_path, skiprows=1, header=None, names=cols, dtype=str)
            else:
                df = pd.read_csv(file_path, header=None, names=cols, dtype=str)

        if df.empty:
            return None

        df = df.apply(fix_price_columns, axis=1)

        numeric_cols = ['open', 'high', 'low', 'close', 'pe', 'foreign_net', 'fund_net', 'dealer_net', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.dropna(subset=['open', 'high', 'low', 'close', 'foreign_net', 'fund_net', 'dealer_net', 'volume'], inplace=True)
        df = df[df['date'].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)]
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values('date', inplace=True)
        df.reset_index(drop=True, inplace=True)

        if len(df) < MIN_DATA_DAYS:
            return None

        recent_data = df.tail(MIN_DATA_DAYS).copy()
        closes = recent_data['close'].values

        recent_data['total_net'] = (
            recent_data['foreign_net'] + recent_data['fund_net'] + recent_data['dealer_net']
        )
        recent_data['cumulative'] = recent_data['total_net'].cumsum()
        cum_vals = recent_data['cumulative'].values
        last_cum = cum_vals[-1]

        # 條件 B: 最近 3 天三大法人皆買超
        last_3_total = recent_data['total_net'].tail(3)
        cond_B = (last_3_total > 0).all()

        # 條件 C: 近期創新低 + 反彈足夠 + 最近累積淨額連續上升
        global_min = np.min(cum_vals)
        min_idx = np.argmin(cum_vals)
        days_from_min_to_today = len(cum_vals) - 1 - min_idx
        recent_bottom = days_from_min_to_today <= RECENT_BOTTOM_WINDOW
        enough_rebound = (last_cum - global_min) > MIN_REBOUND_AMOUNT
        last_m_rising = True
        if len(cum_vals) >= RISING_CHECK_DAYS:
            last_m = cum_vals[-RISING_CHECK_DAYS:]
            last_m_rising = all(last_m[i] <= last_m[i+1] for i in range(len(last_m)-1))
        cond_C = recent_bottom and enough_rebound and last_m_rising

        # 條件 D: 股價近期創新低 + 最近幾天股價上漲
        low_60 = np.min(closes)
        recent_low_occur = low_60 in closes[-PRICE_RECENT_LOW_WINDOW:]
        last_p_prices = closes[-PRICE_RISING_DAYS:]
        price_rising = (
            len(last_p_prices) >= 2 and
            all(last_p_prices[i] < last_p_prices[i+1] for i in range(len(last_p_prices)-1))
        )
        cond_D = recent_low_occur and price_rising

        # 條件 E: 近期有震盪 + 最近3天買超 + 累積仍為負
        osci_period = recent_data['total_net'].tail(RECENT_OSCILLATION_DAYS)
        osci_vals = osci_period.values
        has_oscillation = (
            np.any(osci_vals > 0) and
            np.any(osci_vals < 0) and
            (np.max(osci_vals) - np.min(osci_vals)) > OSCILLATION_MIN_RANGE
        )
        cumulative_still_negative = last_cum < 0
        cond_E = has_oscillation and cond_B and cumulative_still_negative

        all_pass = cond_B and cond_C and cond_D and cond_E

        # 取最新收盤價與日期
        latest_close = df['close'].iloc[-1] if not df.empty else None
        latest_date = df['date'].iloc[-1].strftime('%Y-%m-%d') if not df.empty else "N/A"

        stock_info = {
            'code': str(df['code'].iloc[0]),
            'name': df['name'].iloc[0],
            'cond_B': cond_B,
            'cond_C': cond_C,
            'cond_D': cond_D,
            'cond_E': cond_E,
            'all_pass': all_pass,
            'latest_close': latest_close,
            'latest_date': latest_date,
            'volume_price_pattern': "未分析",
            'interpretation': ""
        }

        # 若符合策略，進行量價分析
        if all_pass:
            pattern, interp = analyze_volume_price_pattern(df)
            stock_info['volume_price_pattern'] = pattern
            stock_info['interpretation'] = interp

        return stock_info

    except Exception as e:
        print(f"處理 {file_path} 時出錯: {e}")
        return None


def scan_stock_folder(folder_path):
    folder = Path(folder_path)
    if not folder.exists():
        print(f"資料夾 {folder_path} 不存在！")
        return

    # 🔍 先統計總檔數
    all_csv_files = list(folder.glob("*.csv"))
    total_stocks = len(all_csv_files)
    print(f"📁 掃描資料夾: {folder_path}")
    print(f"📊 總共找到 {total_stocks} 檔股票資料\n")

    if total_stocks == 0:
        print("⚠️ 資料夾中沒有任何 .csv 檔案！")
        return

    matched_stocks = []
    for csv_file in all_csv_files:
        result = analyze_stock_file(csv_file)
        if result and result['all_pass']:
            matched_stocks.append(result)
            print(f"✅ 符合: {result['code']} {result['name']}")

    print(f"\n=== 符合「震盪後突發回補 + 股價剛啟動 + 累積仍負」的股票 ===")
    print(f"（參數: 震盪{RECENT_OSCILLATION_DAYS}天, 反彈>{MIN_REBOUND_AMOUNT}張, 股價{PRICE_RECENT_LOW_WINDOW}天內創新低）")
    if matched_stocks:
        for s in matched_stocks:
            print(f"{s['code']} {s['name']} | 收盤價: {s['latest_close']:.2f} ({s['latest_date']})")
            print(f"  ➤ 量價形態: {s['volume_price_pattern']}")
            print(f"  💡 解讀: {s['interpretation']}\n")
    else:
        print("沒有符合條件的股票")


if __name__ == "__main__":
    stock_data_folder = "stock_data"
    scan_stock_folder(stock_data_folder)