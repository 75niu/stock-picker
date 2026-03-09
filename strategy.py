"""
周线爆发选股策略
================
基于布林带扩张 + 放量 + MACD金叉零轴上方的周线级别选股策略。

策略逻辑：
1. BOLL_COND: 布林带上轨上升、中轨上升、下轨下降（布林带开口扩张）
2. VOL_COND: 26周内存在成交量比大于4倍的周（放量）
3. MACD_COND: 3周内存在MACD在零轴上方金叉
"""

import numpy as np
import pandas as pd
import json
import os
import sys
import requests
from datetime import datetime, timedelta
from jinja2 import Template
import traceback
import time
import random


# ============================================================
# 数据获取层 - 使用原始HTTP请求，兼容海外服务器
# ============================================================

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': 'https://finance.sina.com.cn/',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def retry_request(func, max_retries=3, delay=2):
    """带重试的请求包装器"""
    for attempt in range(max_retries):
        try:
            result = func()
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                wait = delay * (attempt + 1) + random.uniform(0, 1)
                print(f"    请求失败({e}), {wait:.1f}秒后重试...")
                time.sleep(wait)
            else:
                raise


def get_all_a_stocks_sina():
    """通过新浪财经接口获取A股股票列表（海外可访问）"""
    print("  尝试新浪财经接口...")
    all_stocks = []

    # 新浪财经分页获取股票列表
    for page in range(1, 120):
        url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=auto"
        try:
            resp = SESSION.get(url, timeout=15)
            if resp.status_code != 200 or not resp.text.strip() or resp.text.strip() == 'null':
                break
            # 新浪返回的是非标准JSON，需要处理
            text = resp.text.strip()
            # 替换键名的单引号为双引号
            import re
            text = re.sub(r"(\w+)\s*:", r'"\1":', text)
            text = text.replace("'", '"')
            data = json.loads(text)
            if not data:
                break
            for item in data:
                code = item.get('code', '') or item.get('symbol', '')
                name = item.get('name', '')
                if code and name:
                    all_stocks.append({'代码': code, '名称': name})
        except Exception as e:
            if page > 5:  # 至少获取了一些数据
                break
            continue
        time.sleep(0.1)

    return pd.DataFrame(all_stocks)


def get_all_a_stocks_eastmoney():
    """通过东方财富HTTP接口获取A股列表"""
    print("  尝试东方财富接口...")
    all_stocks = []

    for page in range(1, 120):
        url = (
            f"https://82.push2.eastmoney.com/api/qt/clist/get?"
            f"pn={page}&pz=50&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281"
            f"&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
            f"&fields=f12,f14&_={int(time.time()*1000)}"
        )
        try:
            resp = SESSION.get(url, timeout=15)
            data = resp.json()
            items = data.get('data', {}).get('diff', [])
            if not items:
                break
            for item in items:
                code = item.get('f12', '')
                name = item.get('f14', '')
                if code and name:
                    all_stocks.append({'代码': code, '名称': name})
        except Exception:
            if page > 5:
                break
            continue
        time.sleep(0.1)

    return pd.DataFrame(all_stocks)


def get_all_a_stocks_akshare():
    """通过akshare获取（国内服务器可用）"""
    print("  尝试akshare接口...")
    import akshare as ak
    stock_info = ak.stock_zh_a_spot_em()
    return stock_info[['代码', '名称']]


def get_all_a_stocks():
    """获取所有A股股票列表 - 多数据源自动切换"""
    print("[1/4] 获取A股股票列表...")

    # 按优先级尝试多个数据源
    sources = [
        ("东方财富HTTP", get_all_a_stocks_eastmoney),
        ("新浪财经", get_all_a_stocks_sina),
        ("akshare", get_all_a_stocks_akshare),
    ]

    for source_name, fetch_func in sources:
        try:
            df = retry_request(fetch_func)
            if df is not None and not df.empty:
                # 过滤掉ST、退市股
                df = df[~df['名称'].str.contains('ST|退', na=False)]
                # 只保留沪深主板、创业板、科创板
                df = df[df['代码'].str.match(r'^(00|30|60|68)')]
                df = df.drop_duplicates(subset='代码').reset_index(drop=True)
                print(f"  [{source_name}] 成功! 共 {len(df)} 只股票待筛选")
                return df
        except Exception as e:
            print(f"  [{source_name}] 失败: {e}")
            continue

    print("  所有数据源均失败!")
    return pd.DataFrame(columns=['代码', '名称'])


def get_weekly_data_eastmoney(stock_code: str) -> pd.DataFrame:
    """通过东方财富HTTP接口获取周线数据"""
    # 确定市场代码
    if stock_code.startswith(('60', '68')):
        secid = f"1.{stock_code}"
    else:
        secid = f"0.{stock_code}"

    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=800)).strftime('%Y%m%d')

    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
        f"secid={secid}&ut=fa5fd1943c7b386f172d6893dbbd4dc0"
        f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&klt=102&fqt=1&beg={start_date}&end={end_date}"
        f"&_={int(time.time()*1000)}"
    )

    resp = SESSION.get(url, timeout=15)
    data = resp.json()
    klines = data.get('data', {}).get('klines', [])

    if not klines:
        return pd.DataFrame()

    rows = []
    for line in klines:
        parts = line.split(',')
        if len(parts) >= 7:
            rows.append({
                'date': parts[0],
                'open': float(parts[1]),
                'close': float(parts[2]),
                'high': float(parts[3]),
                'low': float(parts[4]),
                'vol': float(parts[5]),
                'amount': float(parts[6]),
            })

    df = pd.DataFrame(rows)
    df = df.sort_values('date').reset_index(drop=True)
    return df


def get_weekly_data_sina(stock_code: str) -> pd.DataFrame:
    """通过新浪财经获取周线数据"""
    if stock_code.startswith(('60', '68')):
        symbol = f"sh{stock_code}"
    else:
        symbol = f"sz{stock_code}"

    url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=1200&ma=no&datalen=120"

    resp = SESSION.get(url, timeout=15)
    text = resp.text.strip()
    if not text or text == 'null':
        return pd.DataFrame()

    import re
    text = re.sub(r"(\w+)\s*:", r'"\1":', text)
    text = text.replace("'", '"')
    data = json.loads(text)

    if not data:
        return pd.DataFrame()

    rows = []
    for item in data:
        rows.append({
            'date': item.get('day', ''),
            'open': float(item.get('open', 0)),
            'close': float(item.get('close', 0)),
            'high': float(item.get('high', 0)),
            'low': float(item.get('low', 0)),
            'vol': float(item.get('volume', 0)),
        })

    df = pd.DataFrame(rows)
    df = df.sort_values('date').reset_index(drop=True)
    return df


def get_weekly_data(stock_code: str, name: str) -> pd.DataFrame:
    """获取周线数据 - 多数据源自动切换"""
    sources = [
        get_weekly_data_eastmoney,
        get_weekly_data_sina,
    ]

    for fetch_func in sources:
        try:
            df = fetch_func(stock_code)
            if df is not None and len(df) >= 30:
                return df
        except Exception:
            continue

    return pd.DataFrame()


def get_daily_data_for_display(stock_code: str) -> dict:
    """获取最新日线数据用于页面展示"""
    try:
        if stock_code.startswith(('60', '68')):
            secid = f"1.{stock_code}"
        else:
            secid = f"0.{stock_code}"

        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')

        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
            f"secid={secid}&ut=fa5fd1943c7b386f172d6893dbbd4dc0"
            f"&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
            f"&klt=101&fqt=1&beg={start_date}&end={end_date}"
            f"&_={int(time.time()*1000)}"
        )

        resp = SESSION.get(url, timeout=15)
        data = resp.json()
        klines = data.get('data', {}).get('klines', [])

        if not klines:
            return {}

        latest_parts = klines[-1].split(',')
        prev_parts = klines[-2].split(',') if len(klines) > 1 else latest_parts

        price = float(latest_parts[2])
        prev_close = float(prev_parts[2])
        change_pct = (price - prev_close) / prev_close * 100

        return {
            'price': price,
            'change_pct': round(change_pct, 2),
            'volume': float(latest_parts[5]),
            'turnover': float(latest_parts[6]),
            'high': float(latest_parts[3]),
            'low': float(latest_parts[4]),
            'open': float(latest_parts[1]),
        }
    except Exception:
        return {}


# ============================================================
# 策略计算层
# ============================================================

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def ma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()

def std(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).std(ddof=0)

def ref(series: pd.Series, n: int) -> pd.Series:
    return series.shift(n)

def cross(s1: pd.Series, s2: pd.Series) -> pd.Series:
    return (s1 > s2) & (s1.shift(1) <= s2.shift(1))

def exist(cond: pd.Series, n: int) -> pd.Series:
    return cond.rolling(window=n).max().astype(bool)


def apply_strategy(df: pd.DataFrame) -> pd.Series:
    """
    对周线数据应用"周线爆发"策略
    df 需包含: close, vol 列
    返回布尔Series，True表示当前周满足选股条件
    """
    close = df['close']
    vol = df['vol']

    # --- 布林带条件 ---
    mid = ma(close, 20)
    upper = mid + 2 * std(close, 20)
    lower = mid - 2 * std(close, 20)

    boll_cond = (
        (upper > ref(upper, 1)) &
        (mid > ref(mid, 1)) &
        (lower < ref(lower, 1))
    )

    # --- 成交量条件 ---
    vol_ratio = vol / ref(vol, 1)
    vol_cond = exist(vol_ratio > 4, 26)

    # --- MACD条件 ---
    dif = ema(close, 12) - ema(close, 26)
    dea = ema(dif, 9)
    jc = cross(dif, dea)
    zero_cond = dea > 0
    macd_cond = exist(jc & zero_cond, 3)

    # --- 综合选股 ---
    xg = boll_cond & vol_cond & macd_cond
    return xg


# ============================================================
# 主流程
# ============================================================

def run_strategy():
    """运行完整选股流程"""
    print("=" * 60)
    print(f"  周线爆发选股策略 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    stocks = get_all_a_stocks()
    if stocks.empty:
        print("无法获取股票列表，退出")
        return []

    selected = []
    total = len(stocks)
    failed = 0

    print(f"\n[2/4] 逐只计算策略信号（共 {total} 只）...")
    for idx, row in stocks.iterrows():
        code = row['代码']
        name = row['名称']

        if idx % 200 == 0:
            print(f"  进度: {idx}/{total} ({idx/total*100:.1f}%)")

        df = get_weekly_data(code, name)
        if df.empty:
            failed += 1
            continue

        try:
            signal = apply_strategy(df)
            if signal.iloc[-1]:
                selected.append({
                    'code': code,
                    'name': name,
                })
                print(f"  ★ 选中: {code} {name}")
        except Exception:
            failed += 1
            continue

        # 控制请求频率
        time.sleep(0.12)

    print(f"\n  策略计算完成: 成功 {total - failed}, 失败 {failed}")

    print(f"\n[3/4] 获取选中股票的最新行情...")
    for item in selected:
        daily = get_daily_data_for_display(item['code'])
        item.update(daily)
        time.sleep(0.1)

    print(f"\n  共选出 {len(selected)} 只股票")
    return selected


def generate_html(selected_stocks: list, output_path: str):
    """生成移动端适配的HTML展示页面"""
    print(f"\n[4/4] 生成展示页面...")

    template_str = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>周线爆发选股</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
    background: #0a0e27;
    color: #e0e6ff;
    min-height: 100vh;
    padding-bottom: env(safe-area-inset-bottom);
}
.header {
    background: linear-gradient(135deg, #1a1f4e 0%, #0d1234 100%);
    padding: 20px 16px 16px;
    border-bottom: 1px solid rgba(100, 120, 255, 0.15);
    position: sticky;
    top: 0;
    z-index: 100;
    backdrop-filter: blur(20px);
}
.header h1 {
    font-size: 20px;
    font-weight: 700;
    background: linear-gradient(90deg, #6c8cff, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: 1px;
}
.header .meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 8px;
    font-size: 12px;
    color: #7a85b3;
}
.header .count {
    background: rgba(100, 120, 255, 0.15);
    color: #8fa4ff;
    padding: 2px 10px;
    border-radius: 12px;
    font-weight: 600;
}
.strategy-tag {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background: rgba(167, 139, 250, 0.12);
    color: #a78bfa;
    padding: 4px 10px;
    border-radius: 6px;
    font-size: 11px;
    margin-top: 10px;
}
.strategy-tag::before {
    content: '';
    width: 6px;
    height: 6px;
    background: #a78bfa;
    border-radius: 50%;
    animation: pulse 2s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
}
.stock-list { padding: 12px; }
.stock-card {
    background: linear-gradient(135deg, rgba(26, 31, 78, 0.8) 0%, rgba(13, 18, 52, 0.9) 100%);
    border: 1px solid rgba(100, 120, 255, 0.1);
    border-radius: 14px;
    padding: 16px;
    margin-bottom: 10px;
    transition: all 0.2s;
    position: relative;
    overflow: hidden;
}
.stock-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, rgba(100, 120, 255, 0.3), transparent);
}
.stock-card:active {
    transform: scale(0.98);
    border-color: rgba(100, 120, 255, 0.3);
}
.card-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
}
.stock-name {
    font-size: 17px;
    font-weight: 700;
    color: #e8ecff;
}
.stock-code {
    font-size: 12px;
    color: #5a6599;
    margin-top: 2px;
    font-family: 'SF Mono', 'Fira Code', monospace;
}
.stock-price {
    text-align: right;
}
.price-value {
    font-size: 22px;
    font-weight: 700;
    font-family: 'SF Mono', 'DIN Alternate', monospace;
}
.price-change {
    font-size: 13px;
    font-weight: 600;
    margin-top: 2px;
}
.up { color: #f43f5e; }
.down { color: #10b981; }
.flat { color: #7a85b3; }
.card-bottom {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 8px;
    margin-top: 14px;
    padding-top: 12px;
    border-top: 1px solid rgba(100, 120, 255, 0.08);
}
.metric {
    text-align: center;
}
.metric-label {
    font-size: 10px;
    color: #5a6599;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.metric-value {
    font-size: 13px;
    color: #b0badf;
    margin-top: 2px;
    font-family: 'SF Mono', monospace;
}
.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: #5a6599;
}
.empty-state .icon { font-size: 48px; margin-bottom: 16px; }
.empty-state p { font-size: 14px; line-height: 1.6; }
.footer {
    text-align: center;
    padding: 20px;
    font-size: 11px;
    color: #3d4570;
    border-top: 1px solid rgba(100, 120, 255, 0.06);
    margin-top: 10px;
}
.footer a { color: #5a6599; text-decoration: none; }
.disclaimer {
    background: rgba(234, 179, 8, 0.06);
    border: 1px solid rgba(234, 179, 8, 0.15);
    border-radius: 10px;
    padding: 12px 14px;
    margin: 12px;
    font-size: 11px;
    color: #b8a44a;
    line-height: 1.5;
}
</style>
</head>
<body>
<div class="header">
    <h1>周线爆发选股</h1>
    <div class="meta">
        <span>{{ update_time }}</span>
        <span class="count">{{ stock_count }} 只</span>
    </div>
    <div class="strategy-tag">BOLL扩张 + 放量 + MACD零上金叉</div>
</div>

<div class="disclaimer">
    本页面仅为量化策略筛选结果展示，不构成任何投资建议。股市有风险，投资需谨慎。
</div>

<div class="stock-list">
{% if stocks %}
{% for s in stocks %}
<div class="stock-card">
    <div class="card-top">
        <div>
            <div class="stock-name">{{ s.name }}</div>
            <div class="stock-code">{{ s.code }}</div>
        </div>
        <div class="stock-price">
            {% if s.price %}
            <div class="price-value {% if s.change_pct > 0 %}up{% elif s.change_pct < 0 %}down{% else %}flat{% endif %}">
                {{ "%.2f"|format(s.price) }}
            </div>
            <div class="price-change {% if s.change_pct > 0 %}up{% elif s.change_pct < 0 %}down{% else %}flat{% endif %}">
                {% if s.change_pct > 0 %}+{% endif %}{{ "%.2f"|format(s.change_pct) }}%
            </div>
            {% else %}
            <div class="price-value flat">--</div>
            {% endif %}
        </div>
    </div>
    {% if s.price %}
    <div class="card-bottom">
        <div class="metric">
            <div class="metric-label">开盘</div>
            <div class="metric-value">{{ "%.2f"|format(s.open) }}</div>
        </div>
        <div class="metric">
            <div class="metric-label">最高</div>
            <div class="metric-value">{{ "%.2f"|format(s.high) }}</div>
        </div>
        <div class="metric">
            <div class="metric-label">最低</div>
            <div class="metric-value">{{ "%.2f"|format(s.low) }}</div>
        </div>
    </div>
    {% endif %}
</div>
{% endfor %}
{% else %}
<div class="empty-state">
    <div class="icon">📊</div>
    <p>今日暂无符合"周线爆发"策略的股票<br>策略会在每周交易日自动更新</p>
</div>
{% endif %}
</div>

<div class="footer">
    <p>策略自动运行 · 数据来源: 东方财富</p>
    <p style="margin-top:4px;">周线级别 · 每个交易日收盘后自动更新</p>
</div>
</body>
</html>"""

    template = Template(template_str)
    html = template.render(
        stocks=selected_stocks,
        stock_count=len(selected_stocks),
        update_time=datetime.now().strftime('%Y年%m月%d日 %H:%M 更新'),
    )

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  页面已生成: {output_path}")


def save_data_json(selected_stocks: list, output_path: str):
    """保存选股结果为JSON"""
    data = {
        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'strategy': '周线爆发',
        'count': len(selected_stocks),
        'stocks': selected_stocks,
    }
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  数据已保存: {output_path}")


if __name__ == '__main__':
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docs')
    os.makedirs(output_dir, exist_ok=True)

    results = run_strategy()

    html_path = os.path.join(output_dir, 'index.html')
    generate_html(results, html_path)

    json_path = os.path.join(output_dir, 'data.json')
    save_data_json(results, json_path)

    print(f"\n{'=' * 60}")
    print(f"  完成! 共选出 {len(results)} 只股票")
    print(f"  HTML: {html_path}")
    print(f"  JSON: {json_path}")
    print(f"{'=' * 60}")
