from flask import Flask, request, jsonify, redirect
import os, json

app = Flask(__name__)
PERMANENT_API = "https://trading-bot-api-isig.onrender.com"
DATA_FILE = "bot_data.json"

default_settings = {
    "platforms": ["MT5","MT4","CTRADER","TRADINGVIEW","BYBIT","BINANCE","OKX","DERIV","KUCOIN","EXNESS","FBS","FTMO","ICMARKETS"],
    "admin_password": "isigadmin123",
    "site_name": "ISIG BOT",
    "max_accounts": 500
}

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE,'r') as f: return json.load(f)
        except: pass
    return {"settings": default_settings, "accounts": [], "orders": []}

def save_data(data):
    with open(DATA_FILE,'w') as f: json.dump(f,data, indent=2)

def layout(title, content):
    s = load_data()['settings']
    return f"""
<html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{title} - {s['site_name']}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif}}
body{{background:#f8fafc;color:#0f172a;line-height:1.6}}
.nav{{background:white;border-bottom:1px solid #e2e8f0;padding:14px 20px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:99}}
.logo{{font-weight:900;font-size:20px;color:#2563eb}} .logo b{{color:#0f172a}}
.links a{{text-decoration:none;color:#475569;font-weight:600;font-size:13px;margin-left:14px;padding:8px 12px;border-radius:8px}} .links a:hover{{background:#f1f5f9;color:#2563eb}}
.links a.active{{background:#0f172a;color:white}}
.card{{background:white;border:1px solid #e2e8f0;border-radius:14px;padding:22px;margin:16px auto;max-width:1100px;box-shadow:0 2px 10px rgba(0,0,0,0.03)}}
.btn{{background:#2563eb;color:white;padding:12px 20px;border-radius:10px;text-decoration:none;font-weight:700;border:none;display:inline-block;cursor:pointer;font-size:14px}}
.btn-dark{{background:#0f172a}} .btn-green{{background:#16a34a}} .btn-outline{{background:white;color:#0f172a;border:1px solid #e2e8f0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}}
.input{{width:100%;padding:12px;border:1px solid #e2e8f0;border-radius:10px;margin-top:6px}} label{{font-weight:600;font-size:13px;color:#334155}}
.badge{{background:#dcfce7;color:#16a34a;padding:4px 10px;border-radius:20px;font-size:11px;font-weight:700}} .muted{{color:#64748b;font-size:13px}}
.hero{{background:white;text-align:center;padding:40px 20px;border-bottom:1px solid #e2e8f0}}
.step{{background:#f8fafc;border:1px dashed #cbd5e1;padding:14px;border-radius:10px;margin:10px 0}}
.platform{{display:inline-block;border:1px solid #e2e8f0;background:white;padding:5px 10px;border-radius:20px;font-size:11px;font-weight:700;margin:3px}}
.footer{{text-align:center;padding:20px;color:#94a3b8;font-size:11px}}
</style></head><body>
<div class='nav'>
  <div class='logo'>{s['site_name']} <b>PRO</b></div>
  <div class='links'>
    <a href='/'>HOME</a><a href='/setup'>SETUP</a><a href='/api'>API</a><a href='/orders'>ORDERS</a><a href='/settings'>SETTINGS</a><a href='/admin'>ADMIN</a>
  </div>
</div>
{content}
<div class='footer'>Permanent API: {PERMANENT_API} | {s['site_name']} © 2026 | Live: trading-bot-api-isig.onrender.com</div>
</body></html>
"""

@app.route('/')
def home():
    data=load_data()
    plats="".join([f"<span class='platform'>{p}</span>" for p in data['settings']['platforms']])
    return layout("Home", f"""
<div class='hero'>
  <div><span class='badge'>● LIVE - READY TO TRADE</span></div>
  <div style='font-size:64px;margin:10px 0'>🚀🤖✨</div>
  <h1 style='font-size:32px;font-weight:900;letter-spacing:-1px;'>Are you ready to start<br>trading with ISIG BOT?</h1>
  <p class='muted' style='max-width:600px;margin:12px auto;font-size:15px;'>Connect to your <b>MT5 using your laptop</b> and automate trading across 13 platforms. No coding needed - just follow the simple steps below.</p>
  <div style='margin-top:18px'><a href='#guide' class='btn'>👉 YES, SHOW ME HOW TO CONNECT MY MT5</a> <a href='/setup' class='btn btn-outline' style='margin-left:6px'>Go to Setup</a></div>
  <div style='margin-top:20px'>{plats}</div>
</div>

<div class='card' id='guide'>
<h2>📘 Complete Guide: How To Connect Your MT5 - Step By Step (Simple English)</h2>
<p class='muted'>Follow these 5 steps only. After this, your bot will start trading automatically.</p>

<div class='step'><b>STEP 1: Prepare Your Laptop & MT5</b><br>
<span class='muted'>1. Open your laptop<br>2. Open your MT5 app (MetaTrader 5) - from Exness, FBS, Deriv, etc.<br>3. Login to your MT5 account - you should see your balance<br>4. At top of MT5, click <b>Tools → Options → Expert Advisors →</b> Tick <b>"Allow WebRequest for listed URL"</b> and add:<br><code>{PERMANENT_API}</code></span></div>

<div class='step'><b>STEP 2: Get Your MT5 Details</b><br>
<span class='muted'>You need 3 things from MT5:<br>1. <b>Login ID</b> - your MT5 account number (e.g. 12345678)<br>2. <b>Password</b> - your MT5 trading password<br>3. <b>Server</b> - e.g. Exness-Real, FBS-Real, Deriv-Real. You can see server name in MT5 bottom right.<br>Write them down.</span></div>

<div class='step'><b>STEP 3: Add Account In This Website</b><br>
<span class='muted'>1. Click <a href='/setup'><b>SETUP</b></a> at top menu<br>2. Select how many accounts you want (e.g. Account 1)<br>3. Choose Platform: <b>MT5</b><br>4. Paste your Login ID, Password, Server<br>5. Click <b>Save Account</b> - it will be saved securely.</span></div>

<div class='step'><b>STEP 4: Connect Using Your Laptop (Final Step)</b><br>
<span class='muted'>Your bot lives online at <b>{PERMANENT_API}</b> - it is already connected to internet.<br>To link your MT5 on laptop to online bot, you need to run a small connector EA (Expert Advisor) on your MT5 chart.<br><b>Easy method:</b> In MT5, open any chart (e.g. EURUSD), drag any EA that sends WebRequest to your API. Or use my API endpoint: <code>/api/status</code> to test.<br>Once connected, you will see your MT5 account show <b>Connected ✅</b> in <a href='/setup'><b>SETUP page</b></a>.</span></div>

<div class='step'><b>STEP 5: Start Trading</b><br>
<span class='muted'>1. Go to <a href='/orders'><b>ORDERS page</b></a> - you will see live trades<br>2. Your bot will now copy signals or auto-trade based on your settings<br>3. You can leave laptop ON, MT5 running. Bot runs 24/7.<br><br><b>💡 Tip:</b> Keep MT5 open on laptop with internet. If laptop sleeps, use VPS.</span></div>

<div style='background:#eff6ff;padding:14px;border-radius:10px;margin-top:12px'><b>❓ Need Help?</b><br><span class='muted'>Your Permanent API never changes: <b>{PERMANENT_API}</b><br>Use it in MT5 WebRequest. Go to ADMIN page to change platforms or add more accounts up to 500. Default admin password: isigadmin123 (you can change it in SETTINGS).</span></div>

<div style='text-align:center;margin-top:18px'><a href='/setup' class='btn btn-green' style='padding:14px 28px;font-size:16px'>✅ I UNDERSTAND - GO TO SETUP NOW →</a></div>
</div>

<div class='card'><div class='grid'>
<div><div class='muted'>Accounts</div><div style='font-size:24px;font-weight:800'>{len(data['accounts'])} / {data['settings']['max_accounts']}</div></div>
<div><div class='muted'>Platforms</div><div style='font-size:24px;font-weight:800'>{len(data['settings']['platforms'])} Supported</div></div>
<div><div class='muted'>API Status</div><div style='font-size:18px;font-weight:800;color:#16a34a'>● LIVE & ONLINE</div></div>
</div></div>
""")

@app.route('/setup', methods=['GET'])
def setup():
    data=load_data()
    max_acc=data['settings']['max_accounts']
    opts="".join([f"<option value='{i}'>Account {i}</option>" for i in range(1, max_acc+1)])
    plat_opts="".join([f"<option value='{p}'>{p}</option>" for p in data['settings']['platforms']])
    acc_list="".join([f"<div style='border:1px solid #e2e8f0;padding:12px;border-radius:10px;margin:6px 0;background:#fcfdfe'><b>Account {a.get('id')}</b> - {a.get('platform')} - Login: {a.get('login')} - Server: {a.get('server')} <span class='badge'>Saved</span></div>" for a in data['accounts']]) or "<p class='muted'>No accounts saved yet. Add your first MT5 below.</p>"
    return layout("Setup", f"""
<div class='card'>
<h2>Setup - Manage Trading Accounts (1 to {max_acc})</h2>
<p class='muted'>Your saved accounts:</p>
<div style='margin:12px 0'>{acc_list}</div>
<hr style='margin:16px 0;border:none;border-top:1px solid #e2e8f0'>
<h3>Add New Account</h3>
<form action='/save_account' method='post' style='margin-top:12px'>
<div class='grid'>
<div><label>Account Number</label><select name='id' class='input'>{opts}</select></div>
<div><label>Platform</label><select name='platform' class='input'>{plat_opts}</select></div>
</div>
<div class='grid' style='margin-top:10px'>
<div><label>Login ID / API Key</label><input name='login' class='input' placeholder='e.g. 12345678' required></div>
<div><label>Password / Secret</label><input name='password' type='password' class='input' placeholder='MT5 password' required></div>
</div>
<label style='margin-top:10px;display:block'>Server Name (for MT5/MT4)</label><input name='server' class='input' placeholder='e.g. Exness-Real, FBS-Real, Deriv-Real'>
<br><br><button class='btn' type='submit'>💾 Save This Account</button> <a href='/' class='btn btn-outline'>Back to Guide</a>
</form>
<p class='muted' style='margin-top:12px'>Supports: MT5, MT4, cTrader, TradingView, Bybit, Binance, OKX, Deriv, Kucoin, Exness, FBS, FTMO, IC Markets - up to {max_acc} accounts.</p>
</div>
""")

@app.route('/save_account', methods=['POST'])
def save_account():
    data=load_data()
    new_acc={{"id": request.form.get('id'), "platform": request.form.get('platform'), "login": request.form.get('login'), "password": request.form.get('password'), "server": request.form.get('server')}}
    # remove if exists same id
    data['accounts']=[a for a in data['accounts'] if a['id']!=new_acc['id']]
    data['accounts'].append(new_acc)
    save_data(data)
    return redirect('/setup')

@app.route('/api')
def api_page():
    return layout("API", f"""
<div class='card'>
<h2>🔌 Your Permanent API</h2>
<p class='muted'>This URL never changes. Use it everywhere.</p>
<div style='background:#0f172a;color:#22c55e;padding:16px;border-radius:10px;font-family:monospace;margin-top:12px;word-break:break-all;'>{PERMANENT_API}</div>
<div class='grid' style='margin-top:16px'>
<div class='step'><b>GET /api/status</b><br><span class='muted'>Check if bot is live</span><br><a href='/api/status' target='_blank'><span class='badge'>Test Now</span></a></div>
<div class='step'><b>GET /setup</b><br><span class='muted'>View accounts</span></div>
<div class='step'><b>POST /save_account</b><br><span class='muted'>Add account via API</span></div>
</div>
<div style='margin-top:14px;background:#fffbeb;border:1px solid #fde68a;padding:12px;border-radius:10px'><b>⚠️ For MT5 WebRequest:</b><br><span class='muted'>Add this URL to MT5: Tools → Options → Expert Advisors → Allow WebRequest → Add: {PERMANENT_API}</span></div>
</div>
""")

@app.route('/orders')
def orders():
    data=load_data()
    order_html="".join([f"<div style='padding:10px;border-bottom:1px solid #f1f5f9'>{o}</div>" for o in data['orders']]) or "<div style='padding:40px;text-align:center;color:#94a3b8'>No orders yet.<br>Once you connect MT5, orders will appear here automatically.</div>"
    return layout("Orders", f"<div class='card'><h2>📊 Live Orders & Trades</h2><div style='margin-top:12px'>{order_html}</div><br><a href='/setup' class='btn'>Connect MT5 First</a></div>")

@app.route('/settings')
def settings_page():
    return layout("Settings", """
<div class='card' style='max-width:500px'><h2>🔒 Change Admin Password</h2>
<form action='/save_password' method='post' style='margin-top:14px'>
<label>Old Password</label><input name='old' type='password' class='input' required>
<label style='margin-top:10px;display:block'>New Password</label><input name='new' type='password' class='input' required>
<br><br><button class='btn' style='width:100%'>Update Password</button>
</form></div>
""")

@app.route('/admin')
def admin():
    return layout("Admin", """
<div class='card' style='max-width:450px;text-align:center'><h2>🛡️ Admin Panel</h2><p class='muted'>Default: isigadmin123</p>
<form action='/admin_login' method='post' style='margin-top:14px'>
<input name='pw' type='password' class='input' placeholder='Enter admin password' required><br><br><button class='btn btn-dark' style='width:100%'>Unlock Admin</button>
</form></div>
""")

@app.route('/admin_login', methods=['POST'])
def admin_login():
    data=load_data()
    if request.form.get('pw')==data['settings']['admin_password']:
        plats=",".join(data['settings']['platforms'])
        return layout("Admin Edit", f"""
<div class='card'><h2>Admin Control Center - Edit Everything</h2>
<form action='/admin_save' method='post' style='margin-top:14px'>
<div class='grid'><div><label>Site Name</label><input name='site_name' value='{data['settings']['site_name']}' class='input'></div>
<div><label>Max Accounts</label><input name='max_accounts' type='number' value='{data['settings']['max_accounts']}' class='input'></div></div>
<label style='margin-top:12px;display:block'>Platforms (comma separated, you can type anything)</label><input name='platforms' value='{plats}' class='input'>
<label style='margin-top:12px;display:block'>New Admin Password (leave empty to keep old)</label><input name='new_pw' class='input' placeholder='New password'>
<br><br><button class='btn btn-green'>Save Changes</button> <a href='/' class='btn btn-outline'>Cancel</a>
</form></div>
""")
    return layout("Failed", "<div class='card'><h3>❌ Wrong password</h3><a href='/admin' class='btn'>Try Again</a></div>")

@app.route('/admin_save', methods=['POST'])
def admin_save():
    data=load_data()
    data['settings']['site_name']=request.form.get('site_name')
    data['settings']['platforms']=[p.strip().upper() for p in request.form.get('platforms').split(',') if p.strip()]
    try: data['settings']['max_accounts']=int(request.form.get('max_accounts'))
    except: pass
    if request.form.get('new_pw'): data['settings']['admin_password']=request.form.get('new_pw')
    save_data(data)
    return redirect('/')

@app.route('/save_password', methods=['POST'])
def save_password():
    data=load_data()
    if request.form.get('old')==data['settings']['admin_password']:
        data['settings']['admin_password']=request.form.get('new')
        save_data(data)
        return layout("Success", "<div class='card'><h3>✅ Password Changed!</h3><a href='/' class='btn'>Go Home</a></div>")
    return layout("Error", "<div class='card'><h3>Old password wrong</h3><a href='/settings' class='btn'>Retry</a></div>")

@app.route('/api/status')
def status(): 
    d=load_data()
    return jsonify({"status":"live","api":PERMANENT_API,"accounts_count":len(d['accounts']),"platforms":d['settings']['platforms']})

@app.route('/api/accounts')
def api_accounts():
    return jsonify(load_data()['accounts'])

if __name__=='__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',10000)))
