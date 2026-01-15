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
# 條件開關
FLAG_VOLUME_SPIKE = True   # 爆量
FLAG_RED_THREE = True      # 紅三兵
FLAG_NET_BUY = True        # 三大法人：3天中至少2天淨買超 > 0


# 資料夾路徑
FOLDER_PATH = "stock_data"
OUTPUT_CHARTS_FOLDER = "output_charts"
FOCUS_STOCKS_CSV = "focus_stocks.csv"  # 追蹤清單檔案

# 資料庫路徑
DB_TSE_PATH = "stock_data/stock_tse_all.db"  # 上市股票資料庫
DB_OTC_PATH = "stock_data/stock_otc_all.db"  # 上櫃股票資料庫

# 爆量參數
VOL_LOOKBACK = 4           # 回看天數
VOL_MULTIPLE = 1.2         # 倍數（相對於前期最高量）
MIN_VOLUME_THRESHOLD = 5000  # 最近一天成交量最低門檻（張數）

# 紅三兵參數
PRICE_LOOKBACK = 3         # 回看天數

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
    """從資料庫獲取所有股票代碼"""
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
    
    return sorted(list(codes))

# ==============================
# 📈 量價戰法分析引擎
# ==============================
def analyze_volume_price_pattern(df):
    """
    量價戰法分析引擎
    根據量價關係、K線型態、趨勢判斷給出操作建議
    
    返回: {
        'signals': [],  # 信號列表
        'action': '',   # 操作建議: '上車'/'重倉'/'減倉'/'清倉'/'觀望'
        'risk_level': '', # 風險等級: '低'/'中'/'高'
        'summary': ''   # 綜合分析
    }
    """
    if len(df) < 10:
        return {'signals': [], 'action': '觀望', 'risk_level': '中', 'summary': '資料不足'}
    
    # 確保數據類型正確
    df = df.copy()
    for col in ['開盤價', '最高價', '最低價', '收盤價', '成交張數']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    signals = []
    action_score = 0  # 正分=看多，負分=看空，0=觀望
    risk_factors = []
    
    # 取最近資料
    recent = df.tail(10).copy()
    latest = recent.iloc[-1]
    prev_1 = recent.iloc[-2] if len(recent) >= 2 else latest
    prev_2 = recent.iloc[-3] if len(recent) >= 3 else latest
    prev_3 = recent.iloc[-4] if len(recent) >= 4 else latest
    
    # ===== 1. 高量判斷 =====
    is_high_volume = False
    high_volume_day = 0
    
    if '成交張數' in recent.columns:
        last_vol = latest['成交張數']
        prev_3_vols = recent.iloc[-4:-1]['成交張數'].values if len(recent) >= 4 else []
        
        if len(prev_3_vols) > 0 and last_vol > max(prev_3_vols):
            is_high_volume = True
            high_volume_day = 1
            signals.append("🔥 高量第1天（觀察）")
            action_score += 0  # 觀望
        
        # 檢查是否為高量第2-3天
        if len(recent) >= 5:
            for i in range(1, 3):
                day_vol = recent.iloc[-(i+1)]['成交張數']
                before_vols = recent.iloc[-(i+4):-(i+1)]['成交張數'].values
                if len(before_vols) > 0 and day_vol > max(before_vols):
                    high_volume_day = i + 1
                    break
    
    # ===== 2. 支撐壓力判斷 =====
    support_price = None
    resistance_price = None
    
    if '收盤價' in recent.columns and '開盤價' in recent.columns:
        # 找高量K棒的實體低點作為支撐
        if is_high_volume:
            support_price = min(latest['開盤價'], latest['收盤價'])
        
        # 找過往高點作為壓力
        resistance_price = recent['最高價'].max() if '最高價' in recent.columns else recent['收盤價'].max()
        
        # 判斷目前位置
        current_price = latest['收盤價']
        if support_price is not None and not pd.isna(support_price) and not pd.isna(current_price):
            if current_price > support_price:
                signals.append(f"✓ 在支撐線上方 (支撐:{support_price:.2f})")
                action_score += 2
            elif current_price < support_price:
                signals.append(f"✗ 跌破支撐線 (支撐:{support_price:.2f})")
                risk_factors.append("破支撐")
                action_score -= 5
    
    # ===== 3. K線型態判斷 =====
    # 紅三兵 / 綠三兵
    if len(recent) >= 3:
        last_3_closes = recent.tail(3)['收盤價'].values
        if all(last_3_closes[i] < last_3_closes[i+1] for i in range(2)):
            if support_price is not None and not pd.isna(support_price) and not pd.isna(latest['收盤價']) and latest['收盤價'] > support_price:
                signals.append("🚀 支撐線上方紅三兵（上車）")
                action_score += 5
            else:
                signals.append("📈 紅三兵")
                action_score += 2
        elif all(last_3_closes[i] > last_3_closes[i+1] for i in range(2)):
            if resistance_price and latest['收盤價'] < resistance_price:
                signals.append("📉 壓力線下方綠三兵（減倉）")
                action_score -= 4
                risk_factors.append("綠三兵")
            else:
                signals.append("📉 綠三兵")
                action_score -= 2
    
    # 底分型 / 頂分型（簡化判斷：最近3天中間那天最低/最高）
    if len(recent) >= 3:
        last_3 = recent.tail(3)
        lows = last_3['最低價'].values if '最低價' in last_3.columns else last_3['收盤價'].values
        highs = last_3['最高價'].values if '最高價' in last_3.columns else last_3['收盤價'].values
        
        # 底分型：第2天最低
        if lows[1] < lows[0] and lows[1] < lows[2]:
            if support_price is not None and not pd.isna(support_price) and not pd.isna(latest['收盤價']) and latest['收盤價'] > support_price:
                signals.append("🎯 支撐線上方底分型（上車）")
                action_score += 4
        
        # 頂分型：第2天最高
        if highs[1] > highs[0] and highs[1] > highs[2]:
            if resistance_price is not None and not pd.isna(resistance_price) and not pd.isna(latest['收盤價']) and latest['收盤價'] < resistance_price:
                signals.append("⚠️ 壓力線下方頂分型（減倉）")
                action_score -= 4
                risk_factors.append("頂分型")
    
    # ===== 4. 影線判斷 =====
    if '最高價' in latest and '最低價' in latest and '開盤價' in latest and '收盤價' in latest:
        body_high = max(latest['開盤價'], latest['收盤價'])
        body_low = min(latest['開盤價'], latest['收盤價'])
        body_size = abs(latest['收盤價'] - latest['開盤價'])
        
        upper_shadow = latest['最高價'] - body_high
        lower_shadow = body_low - latest['最低價']
        
        # 上天入地（影線是實體2倍）
        if upper_shadow > body_size * 2 or lower_shadow > body_size * 2:
            signals.append("⚡ 上天入地（觀望）")
            action_score = 0
            risk_factors.append("劇烈波動")
        
        # 下影線
        if lower_shadow > body_size * 0.5:
            if is_high_volume:
                signals.append("💡 高量下影線（機會）")
                action_score += 3
            else:
                signals.append("⚠️ 非高量下影線（風險）")
                action_score -= 2
                risk_factors.append("非高量下影線")
        
        # 上影線碰壓力
        if resistance_price and latest['最高價'] >= resistance_price * 0.98:
            if latest['收盤價'] < body_high:
                signals.append("⚠️ 上影線碰壓力過不去（減倉）")
                action_score -= 3
                risk_factors.append("遇阻回落")
    
    # ===== 5. 趨勢判斷 =====
    # 連續三天高低點下移 = 下跌趨勢
    if len(recent) >= 3:
        last_3 = recent.tail(3)
        if '最高價' in last_3.columns and '最低價' in last_3.columns:
            highs = last_3['最高價'].values
            lows = last_3['最低價'].values
            
            if all(highs[i] > highs[i+1] for i in range(2)) and all(lows[i] > lows[i+1] for i in range(2)):
                signals.append("📉 下跌趨勢確立")
                action_score -= 3
                risk_factors.append("下跌趨勢")
    
    # ===== 6. 量能型態 =====
    if '成交張數' in recent.columns and len(recent) >= 4:
        vols = recent.tail(4)['成交張數'].values
        
        # 梯量判斷
        if all(vols[i] < vols[i+1] for i in range(3)):
            # 上漲梯量
            if latest['收盤價'] > prev_3['收盤價']:
                signals.append("⚠️ 上漲梯量（風險）")
                action_score -= 3
                risk_factors.append("上漲梯量")
        elif all(vols[i] > vols[i+1] for i in range(3)):
            # 下跌梯量（縮量）
            if latest['收盤價'] < prev_3['收盤價']:
                signals.append("💡 下跌梯量（機會）")
                action_score += 2
        
        # 量大實體小
        if is_high_volume and '開盤價' in latest and '收盤價' in latest:
            body_size = abs(latest['收盤價'] - latest['開盤價'])
            price_range = latest['最高價'] - latest['最低價'] if '最高價' in latest else body_size
            if body_size < price_range * 0.3:
                signals.append("⚠️ 量大實體小（有人跑）")
                action_score -= 2
                risk_factors.append("量大實體小")
    
    # ===== 7. 連紅/連綠判斷 =====
    if len(recent) >= 4 and '開盤價' in recent.columns and '收盤價' in recent.columns:
        last_4 = recent.tail(4)
        red_count = sum(last_4['收盤價'] > last_4['開盤價'])
        
        if red_count >= 4:
            # 檢查第5天是否放量收綠
            if len(recent) >= 5:
                if latest['收盤價'] < latest['開盤價'] and is_high_volume:
                    signals.append("🚨 連紅≥4天見綠放量（減倉）")
                    action_score -= 4
                    risk_factors.append("連紅後放量收綠")
    
    # ===== 8. 高量特殊規則 =====
    if is_high_volume:
        if high_volume_day == 1:
            signals.append("📋 高量第1天：觀察為主")
            action_score = 0
        elif high_volume_day in [2, 3]:
            if support_price is not None and not pd.isna(support_price) and not pd.isna(latest['收盤價']) and not pd.isna(latest['開盤價']) and latest['收盤價'] > support_price:
                if latest['收盤價'] > latest['開盤價']:
                    signals.append("✨ 高量第2-3天支撐線上方（陽上陰觀）")
                    action_score += 3
    
    # ===== 綜合判斷 =====
    if action_score >= 5:
        action = "上車"
        risk_level = "低"
    elif action_score >= 8:
        action = "重倉"
        risk_level = "低"
    elif action_score <= -5:
        action = "減倉"
        risk_level = "高"
    elif action_score <= -8:
        action = "清倉"
        risk_level = "高"
    else:
        action = "觀望"
        risk_level = "中"
    
    # 強制規則覆蓋
    if "破支撐" in risk_factors:
        action = "清倉"
        risk_level = "高"
    
    # 生成綜合分析
    summary_parts = []
    if signals:
        summary_parts.append(f"發現 {len(signals)} 個信號")
    if risk_factors:
        summary_parts.append(f"風險因子: {', '.join(risk_factors)}")
    summary_parts.append(f"建議: {action}")
    
    summary = " | ".join(summary_parts)
    
    return {
        'signals': signals,
        'action': action,
        'risk_level': risk_level,
        'summary': summary,
        'score': action_score
    }

# ==============================
# 🔧 讀取公司清單（無標題列）
# ==============================
def load_company_lists():
    """
    讀取公司清單，優先順序：
    1. tse_company_list.csv - 基礎上市公司資料（代碼、名稱）
    2. tse_concept_stocks.csv - 上市概念股資料（代碼、名稱、概念股領域）
    3. otc_company_list.csv - 基礎上櫃公司資料（代碼、名稱）
    4. otc_concept_stocks.csv - 上櫃概念股資料（代碼、名稱、概念股領域）
    """
    company_info = {}

    # 第一步：讀取基礎上市公司清單 tse_company_list.csv
    tse_company_path = Path("tse_company_list.csv")
    if tse_company_path.exists():
        try:
            tse_company_df = pd.read_csv(tse_company_path, dtype=str)
            for _, row in tse_company_df.iterrows():
                code = str(row['代號']).strip() if '代號' in row else str(row.iloc[0]).strip()
                if code and (code.isdigit() or (code.endswith('B') and code[:-1].isdigit())):  # 支援一般股票和ETF
                    name = str(row['名稱']).strip() if '名稱' in row else (str(row.iloc[1]).strip() if len(row) > 1 else '未知')
                    company_info[code] = {
                        'name': name,
                        'type': '上市',
                        'sector': '未知'  # 預設值，會被概念股資料覆蓋
                    }
        except Exception as e:
            print(f"⚠️ 讀取 tse_company_list.csv 失敗: {e}")

    # 第二步：讀取上市概念股資料，補充或覆蓋資訊
    tse_concept_path = Path("tse_concept_stocks.csv")
    if tse_concept_path.exists():
        try:
            tse_df = pd.read_csv(tse_concept_path, header=None, dtype=str)
            for _, row in tse_df.iterrows():
                code = str(row[0]).strip()
                if code and (code.isdigit() or (code.endswith('B') and code[:-1].isdigit())):  # 支援一般股票和ETF
                    name = str(row[1]).strip() if len(row) > 1 else '未知'
                    sector = str(row[2]).strip() if len(row) > 2 else '未知'
                    
                    # 如果已存在於 company_info，更新資訊；否則新增
                    if code in company_info:
                        company_info[code]['name'] = name
                        company_info[code]['sector'] = sector
                    else:
                        company_info[code] = {
                            'name': name,
                            'type': '上市',
                            'sector': sector
                        }
        except Exception as e:
            print(f"⚠️ 讀取 tse_concept_stocks.csv 失敗: {e}")

    # 第三步：讀取基礎上櫃公司清單 otc_company_list.csv
    otc_company_path = Path("otc_company_list.csv")
    if otc_company_path.exists():
        try:
            otc_company_df = pd.read_csv(otc_company_path, dtype=str)
            for _, row in otc_company_df.iterrows():
                code = str(row['代號']).strip() if '代號' in row else str(row.iloc[0]).strip()
                if code and (code.isdigit() or (code.endswith('B') and code[:-1].isdigit())):  # 支援一般股票和ETF
                    name = str(row['名稱']).strip() if '名稱' in row else (str(row.iloc[1]).strip() if len(row) > 1 else '未知')
                    company_info[code] = {
                        'name': name,
                        'type': '上櫃',
                        'sector': '未知'  # 預設值，會被概念股資料覆蓋
                    }
        except Exception as e:
            print(f"⚠️ 讀取 otc_company_list.csv 失敗: {e}")

    # 第四步：讀取上櫃概念股資料，補充或覆蓋資訊
    otc_concept_path = Path("otc_concept_stocks.csv")
    if otc_concept_path.exists():
        try:
            otc_df = pd.read_csv(otc_concept_path, header=None, dtype=str)
            for _, row in otc_df.iterrows():
                code = str(row[0]).strip()
                if code and (code.isdigit() or (code.endswith('B') and code[:-1].isdigit())):  # 支援一般股票和ETF
                    name = str(row[1]).strip() if len(row) > 1 else '未知'
                    sector = str(row[2]).strip() if len(row) > 2 else '未知'
                    
                    # 如果已存在於 company_info，更新資訊；否則新增
                    if code in company_info:
                        company_info[code]['name'] = name
                        company_info[code]['sector'] = sector
                    else:
                        company_info[code] = {
                            'name': name,
                            'type': '上櫃',
                            'sector': sector
                        }
        except Exception as e:
            print(f"⚠️ 讀取 otc_concept_stocks.csv 失敗: {e}")

    return company_info

# ==============================
# 📊 分析單檔股票
# ==============================
def analyze_stock(stock_code, vol_lookback=None, vol_multiple=None, min_volume_threshold=None):
    """
    分析單檔股票是否符合條件
    
    參數:
        stock_code: 股票代碼
        vol_lookback: 爆量回看天數（None則使用全局 VOL_LOOKBACK）
        vol_multiple: 爆量倍數（None則使用全局 VOL_MULTIPLE）
        min_volume_threshold: 最低成交量門檻（None則使用全局 MIN_VOLUME_THRESHOLD）
    """
    # 使用傳入的參數，若無則使用全局參數
    lookback = vol_lookback if vol_lookback is not None else VOL_LOOKBACK
    multiple = vol_multiple if vol_multiple is not None else VOL_MULTIPLE
    min_threshold = min_volume_threshold if min_volume_threshold is not None else MIN_VOLUME_THRESHOLD
    
    try:
        # 從資料庫讀取資料
        df = read_stock_from_db(stock_code)
        if df is None or len(df) == 0:
            return None

        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        # 移除千位分隔符逗號後再轉換數值
        for col in ['成交張數', '收盤價', '外陸資買賣超張數', '投信買賣超張數', '自營商買賣超張數']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df.dropna(subset=['日期', '成交張數', '收盤價'], inplace=True)
        df.sort_values('日期', inplace=True)
        df.reset_index(drop=True, inplace=True)

        if len(df) < max(lookback, PRICE_LOOKBACK):
            return None

        latest_date = df['日期'].iloc[-1].strftime('%Y-%m-%d')
        latest_close = df['收盤價'].iloc[-1]

        # ===== 條件 1：爆量 =====
        meets_volume = True
        last_vol_val = None
        max_prev_vol = None
        vol_multiple_result = None

        if FLAG_VOLUME_SPIKE:
            recent_vol = df.tail(lookback)
            vols = recent_vol['成交張數'].values
            last_vol_val = vols[-1]
            prev_vols = vols[:-1]

            if not (last_vol_val > 0 and all(v > 0 for v in prev_vols)):
                meets_volume = False
            elif not all(last_vol_val > v for v in prev_vols):
                meets_volume = False
            else:
                max_prev_vol = max(prev_vols)
                if max_prev_vol <= 0 or last_vol_val < max_prev_vol * multiple:
                    meets_volume = False
                else:
                    vol_multiple_result = round(last_vol_val / max_prev_vol, 2)
        else:
            meets_volume = True

        # ===== 檢查成交量門檻 =====
        # 無論是否啟用爆量條件，都檢查最近一天成交量是否達到門檻
        if last_vol_val is None:
            last_vol_val = df['成交張數'].iloc[-1]
        
        if last_vol_val < min_threshold:
            meets_volume = False

        # ===== 條件 2 + 3：紅三兵 + 三大法人 =====
        meets_red_three = True
        meets_net_buy = True
        closes = None
        net_summary = None

        if FLAG_RED_THREE or FLAG_NET_BUY:
            recent_df = df.tail(PRICE_LOOKBACK)
            if len(recent_df) != PRICE_LOOKBACK:
                meets_red_three = False
                meets_net_buy = False
            else:
                prices = recent_df['收盤價'].values
                c1, c2, c3 = prices[0], prices[1], prices[2]
                closes = (round(c1, 2), round(c2, 2), round(c3, 2))

                if FLAG_RED_THREE:
                    if not (c1 < c2 < c3):
                        meets_red_three = False
                else:
                    meets_red_three = True

                if FLAG_NET_BUY:
                    foreign = recent_df['外陸資買賣超張數'].values
                    trust = recent_df['投信買賣超張數'].values
                    dealer = recent_df['自營商買賣超張數'].values

                    positive_days = 0
                    details = []

                    for f, t, d in zip(foreign, trust, dealer):
                        total = f + t + d
                        if total > 0:
                            positive_days += 1
                        details.append((f, t, d, total))

                    net_summary = {
                        'details': details,
                        'positive_days': positive_days
                    }

                    if positive_days < 2:
                        meets_net_buy = False
                else:
                    meets_net_buy = True
        else:
            meets_red_three = True
            meets_net_buy = True

        if meets_volume and meets_red_three and meets_net_buy:
            result = {
                'code': stock_code,
                'latest_date': latest_date,
                'latest_close': latest_close,
            }
            if FLAG_VOLUME_SPIKE:
                result.update({
                    'last_volume': int(last_vol_val),
                    'max_prev_volume': int(max_prev_vol),
                    'multiple': vol_multiple_result
                })
            if FLAG_RED_THREE:
                result['closes'] = closes
            if FLAG_NET_BUY:
                result['net_summary'] = net_summary
            return result

    except Exception as e:
        print(f"⚠️ 處理股票 {stock_code} 時出錯: {e}")
    return None

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
        
        # ===== 先執行量價分析 =====
        analysis = analyze_volume_price_pattern(df)
        
        # 根據操作建議決定檔案名稱（加入收盤價）
        action = analysis['action']
        
        # 根據是否為概念股模式決定檔名格式
        if industry_category:
            # 概念股模式：產業分類_股票代號_股票名稱_最新收盤價_操作建議.html
            output_filename = f"{industry_category}_{stock_code}_{stock_name}_{latest_close_str}_{action}.html"
        else:
            # 一般模式：操作建議_股票代號_股票名稱_收盤價.html
            output_filename = f"{action}_{stock_code}_{stock_name}_{latest_close_str}.html"
        
        output_path = output_folder / output_filename
        
        # 取最近60筆資料
        df_chart = df.tail(60).copy()
        
        # 計算移動平均線
        df_chart['MA5'] = df_chart['收盤價'].rolling(window=5, min_periods=1).mean()
        df_chart['MA10'] = df_chart['收盤價'].rolling(window=10, min_periods=1).mean()
        
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
        
        # 添加MA5和MA10
        for ma_name, ma_col, color in [('MA5', 'MA5', 'blue'), ('MA10', 'MA10', 'orange')]:
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
        latest_date_str = latest['日期'].strftime('%Y-%m-%d')
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
    """生成單檔股票的HTML圖表"""
    try:
        # 讀取資料
        df = pd.read_csv(csv_file, encoding='utf-8')
        
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
        
        # 取最近60筆資料
        df_chart = df.tail(60).copy()
        
        # 計算移動平均線
        df_chart['MA5'] = df_chart['收盤價'].rolling(window=5, min_periods=1).mean()
        df_chart['MA10'] = df_chart['收盤價'].rolling(window=10, min_periods=1).mean()
        
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
        
        # 添加MA5和MA10
        for ma_name, ma_col, color in [('MA5', 'MA5', 'blue'), ('MA10', 'MA10', 'orange')]:
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
        latest_date_str = latest['日期'].strftime('%Y-%m-%d')
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
        
        # 執行量價分析
        analysis = analyze_volume_price_pattern(df)
        
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
    <title>{stock_code} {stock_name}</title>
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
                操作建議 TEXT,
                風險等級 TEXT,
                評分 INTEGER,
                走勢分析 TEXT,
                信號列表 TEXT,
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
            
            # 只對最近的資料進行量價分析（節省運算時間）
            analysis = None
            if len(stock_df) >= 10:
                analysis = analyze_volume_price_pattern(stock_df)
            
            # 將每一天的資料都寫入資料庫
            for idx, row in stock_df.iterrows():
                # 轉換日期格式為統一格式 YYYY.MM.DD
                try:
                    date_obj = pd.to_datetime(row['日期'])
                    date_str = date_obj.strftime('%Y.%m.%d')
                except:
                    date_str = str(row['日期'])
                
                # 對於最後一天的資料，附加量價分析結果
                is_latest = (idx == stock_df.index[-1])
                if is_latest and analysis:
                    action = analysis['action']
                    risk_level = analysis['risk_level']
                    score = analysis.get('score', 0)
                    summary = analysis.get('summary', '')
                    signals = json.dumps(analysis.get('signals', []), ensure_ascii=False)
                else:
                    # 歷史資料不進行分析，留空
                    action = ''
                    risk_level = ''
                    score = 0
                    summary = ''
                    signals = '[]'
                
                # 判斷是否為 focus 股票
                is_focus = 1 if (focus_stock_codes and code in focus_stock_codes) else 0
                
                cursor.execute('''
                    INSERT OR REPLACE INTO hot_stocks 
                    (股票代碼, 股票名稱, 類型, 產業分類, 日期, 
                     開盤價, 最高價, 最低價, 收盤價, 成交量,
                     成交筆數, 成交金額, 本益比,
                     外陸資買賣超張數, 投信買賣超張數, 自營商買賣超張數,
                     操作建議, 風險等級, 評分, 走勢分析, 信號列表, 更新時間, IS_FOCUS)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    action, risk_level, score, summary, signals, update_time, is_focus
                ))
                total_records += 1
        
        conn.commit()
        conn.close()
        
        if is_first_stage:
            print(f"\n✅ 第一階段：已保存 {len(results)} 檔股票（共 {total_records} 筆交易記錄）到 {db_path}")
            print(f"   資料庫狀態：全新創建，包含完整交易歷史\n")
        else:
            print(f"\n✅ 第二階段：已追加 {len(results)} 檔股票（共 {total_records} 筆交易記錄）到 {db_path}\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 保存到資料庫失敗: {e}\n")
        import traceback
        traceback.print_exc()
        return False

# ==============================
# 🎯 主程式 - 兩階段執行
# ==============================
def main():
    # 檢查資料庫檔案是否存在
    if not Path(DB_TSE_PATH).exists() and not Path(DB_OTC_PATH).exists():
        print(f"❌ 找不到資料庫檔案：{DB_TSE_PATH} 或 {DB_OTC_PATH}")
        return

    # 建立輸出資料夾
    base_output_folder = Path(OUTPUT_CHARTS_FOLDER)
    base_output_folder.mkdir(exist_ok=True)
    
    # 加載公司資訊
    company_info = load_company_lists()
    
    # 從資料庫讀取最新日期
    latest_date_str = None
    try:
        if Path(DB_TSE_PATH).exists():
            conn = sqlite3.connect(DB_TSE_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(日期) FROM stock_data")
            result = cursor.fetchone()
            if result and result[0]:
                latest_date = pd.to_datetime(result[0])
                latest_date_str = latest_date.strftime('%Y.%m.%d')
            conn.close()
        
        if latest_date_str:
            print(f"📅 最新資料日期: {latest_date_str}")
        else:
            from datetime import datetime
            latest_date_str = datetime.now().strftime('%Y.%m.%d')
    except Exception as e:
        print(f"⚠️ 無法讀取日期，使用當前日期: {e}")
        from datetime import datetime
        latest_date_str = datetime.now().strftime('%Y.%m.%d')
    
    # 建立以日期命名的子資料夾
    output_folder = base_output_folder / latest_date_str
    output_folder.mkdir(exist_ok=True)
    
    # 讀取 focus_stocks.csv 取得追蹤股票代碼
    focus_stock_codes = set()
    focus_csv_path = Path(FOCUS_STOCKS_CSV)
    if focus_csv_path.exists():
        try:
            focus_df = pd.read_csv(focus_csv_path, encoding='utf-8')
            focus_df = focus_df.drop_duplicates(subset=['股票代碼'], keep='first')
            focus_stock_codes = set(focus_df['股票代碼'].astype(str).values)
            print(f"📋 已讀取 {len(focus_stock_codes)} 檔追蹤股票")
        except Exception as e:
            print(f"⚠️ 讀取追蹤清單失敗: {e}")
    
    # ==========================================
    # 第一階段：一般模式（掃描所有股票）
    # ==========================================
    print("=" * 70)
    print("🔍 第一階段：一般模式")
    print("=" * 70)
    
    stock_codes = get_all_stock_codes()
    if not stock_codes:
        print(f"📁 資料庫中沒有股票資料！")
        return
    
    print(f"📁 輸出資料夾: {output_folder}\n")

    enabled = []
    if FLAG_VOLUME_SPIKE: enabled.append("爆量")
    if FLAG_RED_THREE: enabled.append("紅三兵")
    if FLAG_NET_BUY: enabled.append("三大法人≥2天淨買超")
    
    print(f"🔍 掃描 {len(stock_codes)} 檔股票...")
    print(f"   • 啟用條件: {' + '.join(enabled) if enabled else '無'}")
    print(f"   • 圖表篩選: 只生成「上車」建議的股票\n")

    # 篩選符合條件的股票
    results = []
    for stock_code in stock_codes:
        res = analyze_stock(stock_code)
        if res:
            results.append(res)

    if FLAG_VOLUME_SPIKE:
        results.sort(key=lambda x: x.get('last_volume', 0), reverse=True)

    print("=" * 70)
    all_codes_stage1 = set()  # 用於記錄第一階段處理的股票代碼
    
    if results:
        print(f"✅ 找到 {len(results)} 檔符合基本條件，將進一步篩選「上車」建議：\n")
        
        chart_count = 0
        for r in results:
            code = r['code']
            all_codes_stage1.add(code)
            info = company_info.get(code, {})
            name = info.get('name', '未知')
            type_str = info.get('type', '未知')
            sector = info.get('sector', '未知')

            print(f"{code} | {name} | {type_str} | {sector} | 日期: {r['latest_date']} | 收盤: {r['latest_close']:.2f}")
            if 'last_volume' in r:
                print(f"    ▲ 成交量: {r['last_volume']:,} 張 (前高 {r['max_prev_volume']:,}, {r['multiple']}x)")
            if 'closes' in r:
                c = r['closes']
                print(f"    📈 紅三兵: {c[0]} → {c[1]} → {c[2]}")
            if 'net_summary' in r:
                summary = r['net_summary']
                print(f"    💰 三大法人合計買超（外/投/自 → 合計）：")
                for i, (f, t, d, total) in enumerate(summary['details'], start=1):
                    sign = "🔴" if total <= 0 else "🟢"
                    print(f"        第{i}天： {int(f):>3} / {int(t):>3} / {int(d):>3} → {int(total):>+6} 張 {sign}")
                print(f"        ▸ 合計 >0 天數：{summary['positive_days']}/3")
            
            # 執行量價分析，只生成「上車」建議的圖表
            stock_df = read_stock_from_db(code)
            if stock_df is not None and len(stock_df) >= 10:
                analysis = analyze_volume_price_pattern(stock_df)
                action = analysis['action']
                print(f"    📊 量價分析: {action}")
                
                if action in ['上車', '重倉', '觀望']:
                    print(f"    🎨 生成圖表...")
                    if generate_stock_chart(code, name, None, output_folder, type_str, sector):
                        chart_count += 1
                else:
                    print(f"    ⏭️  跳過（不是上車建議）")
            else:
                print(f"    ⚠️  資料不足，無法分析")
            
            print()
        
        # 保存到資料庫（第一階段，創建全新資料庫）
        save_to_hot_db(results, company_info, latest_date_str, focus_stock_codes, is_first_stage=True)
        
        print("=" * 70)
        print(f"✅ 第一階段完成：成功生成 {chart_count} 個「上車」建議的圖表")
    else:
        print("❌ 未找到符合所有啟用條件的股票")
    
    # ==========================================
    # 第二階段：追蹤清單模式
    # ==========================================
    print("\n" + "=" * 70)
    print("🎯 第二階段：追蹤清單模式")
    print("=" * 70)
    
    # 讀取追蹤清單
    focus_csv_path = Path(FOCUS_STOCKS_CSV)
    if not focus_csv_path.exists():
        print(f"⚠️ 追蹤清單檔案 '{FOCUS_STOCKS_CSV}' 不存在，跳過第二階段\n")
        print(f"✅ 所有處理完成！輸出資料夾: {output_folder}")
        return
    
    try:
        focus_df = pd.read_csv(focus_csv_path, encoding='utf-8')
        
        # 過濾重複的股票代碼（保留第一次出現）
        original_count = len(focus_df)
        focus_df = focus_df.drop_duplicates(subset=['股票代碼'], keep='first')
        deduplicated_count = len(focus_df)
        
        if original_count > deduplicated_count:
            print(f"📋 讀取到 {original_count} 筆資料，去重後剩餘 {deduplicated_count} 檔股票")
            print(f"   （已過濾 {original_count - deduplicated_count} 個重複項目）\n")
        else:
            print(f"📋 讀取到 {deduplicated_count} 檔追蹤股票\n")
        
        chart_count = 0
        skipped_count = 0
        results_stage2 = []  # 收集第二階段的結果
        
        for idx, row in focus_df.iterrows():
            industry = row['產業分類']
            code = str(row['股票代碼'])
            name = row['股票名稱']
            category = row['領域分類'] if '領域分類' in row else ''
            
            # 過濾重複：如果這支股票在第一階段已處理，跳過
            if code in all_codes_stage1:
                print(f"⏭️  [{idx+1}/{len(focus_df)}] {industry} | {code} {name} - 已在第一階段處理，跳過")
                skipped_count += 1
                print()
                continue
            
            print(f"📊 [{idx+1}/{len(focus_df)}] {industry} | {code} {name}")
            
            # 從資料庫讀取資料
            stock_df = read_stock_from_db(code)
            if stock_df is None or len(stock_df) == 0:
                print(f"    ⚠️ 資料庫中無資料\n")
                continue
            
            # 執行量價分析
            if len(stock_df) >= 10:
                analysis = analyze_volume_price_pattern(stock_df)
                action = analysis['action']
                risk_level = analysis['risk_level']
                score = analysis.get('score', 0)
                
                print(f"    📊 量價分析: {action} | 風險: {risk_level} | 評分: {score}")
                print(f"    💡 {analysis['summary']}")
                
                # 生成圖表（檔名格式與第一階段一致）
                type_str = company_info.get(code, {}).get('type', '未知')
                sector = company_info.get(code, {}).get('sector', '未知')
                
                print(f"    🎨 生成圖表...")
                if generate_stock_chart(code, name, None, output_folder, type_str, sector):
                    chart_count += 1
                    
                    # 收集資料用於保存到資料庫
                    # 確保數據類型正確
                    stock_df_copy = stock_df.copy()
                    for col in ['收盤價', '成交張數']:
                        if col in stock_df_copy.columns:
                            stock_df_copy[col] = stock_df_copy[col].astype(str).str.replace(',', '', regex=False)
                            stock_df_copy[col] = pd.to_numeric(stock_df_copy[col], errors='coerce')
                    
                    stock_df_copy['日期'] = pd.to_datetime(stock_df_copy['日期'], errors='coerce')
                    
                    latest_close = stock_df_copy['收盤價'].iloc[-1] if '收盤價' in stock_df_copy.columns else 0
                    last_volume = stock_df_copy['成交張數'].iloc[-1] if '成交張數' in stock_df_copy.columns else 0
                    latest_date = stock_df_copy['日期'].iloc[-1] if '日期' in stock_df_copy.columns else pd.Timestamp(latest_date_str)
                    
                    results_stage2.append({
                        'code': code,
                        'latest_date': latest_date.strftime('%Y-%m-%d') if isinstance(latest_date, pd.Timestamp) else latest_date_str,
                        'latest_close': float(latest_close) if not pd.isna(latest_close) else 0,
                        'last_volume': int(last_volume) if not pd.isna(last_volume) else 0
                    })
            else:
                print(f"    ⚠️ 資料不足，無法分析")
            
            print()
        
        # 保存第二階段資料到資料庫（追加模式）
        if results_stage2:
            save_to_hot_db(results_stage2, company_info, latest_date_str, focus_stock_codes, is_first_stage=False)
        
        print("=" * 70)
        print(f"✅ 第二階段完成：")
        print(f"   • 總追蹤股票數: {len(focus_df)}")
        print(f"   • 跳過重複股票: {skipped_count}")
        print(f"   • 成功生成圖表: {chart_count}")
        print(f"   • 輸出資料夾: {output_folder}")
        
    except Exception as e:
        print(f"❌ 讀取追蹤清單失敗: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

