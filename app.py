
from flask import Flask, request, jsonify, render_template_string
import os, json, re

app = Flask(__name__)
PERMANENT_API = "https://trading-bot-api-isig.onrender.com/api"
CONFIG_FILE = "accounts.json"
SETTINGS_FILE = "app_settings.json"

default_settings = {
    "app_name": "ISIG BOT - Multi Platform",
    "platforms": ["mt5","mt4","ctrader","tradingview","bybit","binance","okx","deriv","kucoin","exness","fbs","ftmo","icmarkets"],
    "theme": "gold",
    "risk": "1.5%",
    "max_risk": "2.0%",
    "admin_password": "isigadmin123"
}

if os.path.exists(SETTINGS_FILE):
    try:
        with open(SETTINGS_FILE,'r') as f:
            loaded = json.load(f)
            default_settings.update(loaded)
            app_settings = default_settings
    except:
        app_settings = default_settings
else:
    app_settings = default_settings

def save_settings():
    with open(SETTINGS_FILE,'w') as f:
        json.dump(app_settings,f,indent=2)

def apply_prompt_logic(prompt):
    p = prompt.lower()
    changes = []
    
    # Add platform
    m = re.search(r'add platform (\w+)', p)
    if m:
        plat = m.group(1).lower()
        if plat not in app_settings["platforms"]:
            app_settings["platforms"].append(plat)
            changes.append(f"Added platform: {plat.upper()}")
        else:
            changes.append(f"Platform {plat.upper()} already exists")

    # Remove platform
    m = re.search(r'remove platform (\w+)', p)
    if m:
        plat = m.group(1).lower()
        if plat in app_settings["platforms"]:
            app_settings["platforms"].remove(plat)
            changes.append(f"Removed platform: {plat.upper()}")

    # Change app name
    if "app name to" in p or "change name to" in p:
        m = re.search(r'(?:change name to|app name to) (.+)', prompt, re.IGNORECASE)
        if m:
            new_name = m.group(1).strip()
            app_settings["app_name"] = new_name
            changes.append(f"App name changed to: {new_name}")

    # Change theme/color
    if "color to" in p or "theme to" in p:
        for color in ["gold","blue","green","red","purple","black","orange"]:
            if color in p:
                app_settings["theme"] = color
                changes.append(f"Theme changed to {color}")
                break

    # Change risk
    if "risk to" in p:
        m = re.search(r'risk to (\d+\.?\d*%?)', p)
        if m:
            app_settings["risk"] = m.group(1)
            changes.append(f"Risk changed to {m.group(1)}")

    # Change admin password
    if "admin password to" in p or "change password to" in p:
        m = re.search(r'password to (\S+)', prompt, re.IGNORECASE)
        if m:
            new_pass = m.group(1).strip()
            app_settings["admin_password"] = new_pass
            changes.append(f"Admin password changed to: {new_pass} - SAVE IT!")

    if not changes:
        changes.append("Try: 'Add platform exness', 'Change app name to My Bot', 'Change admin password to 1234', 'Change color to blue', 'Remove platform kucoin'")

    save_settings()
    return changes

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{app_name}}</title>
<style>
body{font-family:Arial;background:#0a0a0a;color:#fff;margin:0;padding-bottom:30px}
nav{display:flex;gap:5px;padding:10px;background:#151515;overflow:auto;position:sticky;top:0;z-index:10}
nav button{background:#222;color:#FFD700;border:1px solid #FFD700;padding:9px 11px;border-radius:20px;white-space:nowrap;font-size:11px}
nav button.active{background:#FFD700;color:#000;font-weight:bold}
.page{display:none;padding:15px}
.page.active{display:block}
input,select,textarea{width:100%;padding:12px;margin:7px 0;background:#222;color:#fff;border:1px solid #444;border-radius:10px;box-sizing:border-box}
.card{border:1px solid #333;padding:14px;border-radius:14px;margin:10px 0;background:#181818}
.btn{background:#FFD700;color:#000;padding:13px;border:none;border-radius:10px;width:100%;font-weight:bold}
.btn-admin{background:#00ff88;color:#000;padding:13px;border:none;border-radius:10px;width:100%;font-weight:bold;margin-top:8px}
.small{font-size:11px;color:#999;word-break:break-all}
.platform-tag{display:inline-block;background:#222;border:1px solid #FFD700;color:#FFD700;padding:4px 8px;border-radius:12px;font-size:10px;margin:2px}
h2{color:#FFD700}
</style>
</head>
<body>
<nav>
<button onclick="showPage('home')" id="btn-home" class="active">Home</button>
<button onclick="showPage('setup')" id="btn-setup">Setup</button>
<button onclick="showPage('server')" id="btn-server">API</button>
<button onclick="showPage('orders')" id="btn-orders">Orders</button>
<button onclick="showPage('settings')" id="btn-settings">Settings</button>
<button onclick="showPage('admin')" id="btn-admin" style="border-color:#00ff88;color:#00ff88">ADMIN</button>
</nav>

<div id="home" class="page active">
<h2>{{app_name}}</h2>
<div class="card">
<p>Status: <b id="botStatus">Checking...</b></p>
<p>Accounts: <b id="accCount">0</b></p>
<p>Platforms:</p>
<div id="platList"></div>
<p class="small">PERMANENT API: https://trading-bot-api-isig.onrender.com/api</p>
<button class="btn" onclick="toggleBot()">START / STOP BOT</button>
</div>
</div>

<div id="setup" class="page">
<h2>Setup Accounts</h2>
<select id="numAccounts" onchange="createBoxes()">
<option value="1">1 Account</option><option value="2">2 Accounts</option><option value="3">3 Accounts</option><option value="5">5 Accounts</option><option value="10">10 Accounts</option><option value="20">20 Accounts</option><option value="50">50 Accounts</option><option value="100">100 Accounts</option><option value="500">500 Accounts - Unlimited</option>
</select>
<div id="accountInputs"></div>
<button class="btn" onclick="saveAccounts()">Save Accounts</button>
</div>

<div id="server" class="page">
<h2>Server Check</h2>
<input id="brokerServer" placeholder="Exness-MT5Real, Bybit-Mainnet, Binance" oninput="checkServer()">
<div id="apiBox" style="display:none" class="card"><h3 style="color:#00ff88">✓ API Allowed - No Laptop Needed</h3><p>Bybit, Binance, OKX, Deriv, TradingView can trade without laptop.</p><button class="btn" onclick="alert('API Connected - No laptop needed!')">Ready to Start Trading?</button></div>
<div id="laptopBox" style="display:none" class="card"><h3 style="color:orange">⚠ Laptop Required</h3><p>Exness, FTMO, IC Markets need MT5 + main.py running 24/7 on laptop.</p></div>
</div>

<div id="orders" class="page"><h2>Live Orders - Recent BOS M30</h2><div class="card"><div id="ordersView" class="small">Waiting for main.py...</div></div><button class="btn" onclick="loadOrders()">Refresh</button></div>

<div id="settings" class="page">
<h2>Settings</h2>
<div class="card"><p><b>App:</b> {{app_name}}</p><p><b>Risk:</b> {{risk}}</p><p><b>Theme:</b> {{theme}}</p><p><b>Platforms:</b> {{platforms_str}}</p><p class="small">Permanent API: https://trading-bot-api-isig.onrender.com/api</p></div>
</div>

<div id="admin" class="page">
<h2 style="color:#00ff88">ADMIN - Prompt to Modify App</h2>
<div class="card">
<input type="password" id="adminPass" placeholder="Admin password (default: isigadmin123)">
<textarea id="promptInput" rows="5" placeholder="Type what you want... e.g.&#10;Add platform exness&#10;Change app name to Ogechi Super Bot&#10;Change admin password to mynew123&#10;Change color to blue&#10;Remove platform kucoin&#10;Change risk to 2%"></textarea>
<button class="btn-admin" onclick="applyPrompt()">Apply Prompt - Modify Now</button>
<div id="promptResult" class="card small" style="display:none"></div>
</div>
<div class="card">
<h4>Quick Buttons</h4>
<button class="btn" style="margin:4px 0" onclick="quickPrompt('Add platform exness')">+ Add Exness</button>
<button class="btn" style="margin:4px 0" onclick="quickPrompt('Add platform ftmo')">+ Add FTMO</button>
<button class="btn" style="margin:4px 0" onclick="quickPrompt('Add platform icmarkets')">+ Add IC Markets</button>
<button class="btn" style="margin:4px 0" onclick="quickPrompt('Change color to blue')">Theme Blue</button>
<button class="btn" style="margin:4px 0" onclick="quickPrompt('Change app name to ISIG Premium Bot')">Rename App</button>
</div>
</div>

<script>
const PERMANENT_API = "https://trading-bot-api-isig.onrender.com/api";
let currentPlatforms = {{platforms_json}};

function showPage(p){
document.querySelectorAll('.page').forEach(e=>e.classList.remove('active'));
document.querySelectorAll('nav button').forEach(e=>e.classList.remove('active'));
document.getElementById(p).classList.add('active');
document.getElementById('btn-'+p).classList.add('active');
}
function createBoxes(){
let num=document.getElementById('numAccounts').value;
let div=document.getElementById('accountInputs'); div.innerHTML="";
let opts = currentPlatforms.map(pl=>`<option value="${pl}">${pl.toUpperCase()}</option>`).join('');
for(let i=1;i<=num;i++){
div.innerHTML+=`<div class="card"><h4>Account ${i}</h4><select id="platform${i}">${opts}</select><input id="login${i}" placeholder="Login / API Key"><input id="pass${i}" type="password" placeholder="Password / Secret"><input id="server${i}" placeholder="Server e.g. Exness-MT5Real"></div>`;
}
document.getElementById('platList').innerHTML = currentPlatforms.map(p=>`<span class="platform-tag">${p.toUpperCase()}</span>`).join('');
}
function saveAccounts(){
let num=document.getElementById('numAccounts').value; let acc=[];
for(let i=1;i<=num;i++){acc.push({platform:document.getElementById(`platform${i}`).value,login:document.getElementById(`login${i}`).value,password:document.getElementById(`pass${i}`).value,server:document.getElementById(`server${i}`).value});}
fetch(`${PERMANENT_API}/api/save_accounts`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({accounts:acc})}).then(r=>r.json()).then(d=>{alert(`Saved ${acc.length} accounts across platforms!`); document.getElementById('accCount').innerText=acc.length;}).catch(()=>{localStorage.setItem('isig_accounts',JSON.stringify(acc)); alert(`Saved ${acc.length} locally - Render sleeping`); document.getElementById('accCount').innerText=acc.length;});
}
function checkServer(){
let s=document.getElementById('brokerServer').value.toLowerCase();
if(["bybit","binance","okx","kucoin","deriv","tradingview"].some(b=>s.includes(b))){document.getElementById('apiBox').style.display="block"; document.getElementById('laptopBox').style.display="none";}
else if(s.length>2){document.getElementById('laptopBox').style.display="block"; document.getElementById('apiBox').style.display="none";}
}
function loadOrders(){document.getElementById('ordersView').innerHTML="Waiting for main.py... If MT5, run main.py on laptop with MT5 open. If API, orders show automatically.";}
function toggleBot(){fetch(`${PERMANENT_API}/api/toggle_bot`,{method:'POST'}).then(r=>r.json()).then(d=>{document.getElementById('botStatus').innerText=d.is_running?"ON":"OFF";}).catch(()=>{document.getElementById('botStatus').innerText="ON (Local)";});}
function quickPrompt(t){document.getElementById('promptInput').value=t; applyPrompt();}
function applyPrompt(){
let pass=document.getElementById('adminPass').value; if(!pass){alert("Enter admin password first! Default is isigadmin123"); return;}
let prompt=document.getElementById('promptInput').value; if(!prompt){alert("Type what you want to change"); return;}
fetch(`${PERMANENT_API}/api/admin_prompt`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({prompt:prompt,password:pass})}).then(r=>r.json()).then(d=>{
if(d.error){alert("Wrong password!"); return;}
let resDiv=document.getElementById('promptResult'); resDiv.style.display="block"; resDiv.innerHTML="<b>Done:</b><br>"+d.changes.join("<br>")+"<br><br>Reloading...";
setTimeout(()=>location.reload(),1500);
}).catch(()=>{alert("Render waking up - tap again in 20 seconds");});
}
createBoxes();
fetch(`${PERMANENT_API}/api/bot_status`).then(r=>r.json()).then(d=>{document.getElementById('botStatus').innerText=d.is_running?"ON":"OFF";}).catch(()=>{document.getElementById('botStatus').innerText="OFF";});
</script>
</body>
</html>
"""

accounts_db = []
bot_running = False

@app.route('/')
def home():
    platforms_str = ", ".join([p.upper() for p in app_settings["platforms"]])
    return render_template_string(HTML_TEMPLATE, 
        app_name=app_settings["app_name"],
        risk=app_settings["risk"],
        theme=app_settings["theme"],
        platforms_str=platforms_str,
        platforms_json=json.dumps(app_settings["platforms"])
    )

@app.route('/api/save_accounts', methods=['POST'])
def save_accounts():
    global accounts_db
    data = request.get_json()
    accounts_db = data.get('accounts', [])
    with open(CONFIG_FILE, 'w') as f:
        json.dump({"accounts": accounts_db}, f, indent=2)
    return jsonify({"status": "saved", "count": len(accounts_db)})

@app.route('/api/get_accounts')
def get_accounts():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE,'r') as f:
            return jsonify(json.load(f))
    return jsonify({"accounts": accounts_db})

@app.route('/api/get_settings')
def get_settings():
    return jsonify(app_settings)

@app.route('/api/admin_prompt', methods=['POST'])
def admin_prompt():
    data = request.get_json()
    if data.get('password') != app_settings.get('admin_password','isigadmin123'):
        return jsonify({"error": "wrong password"}), 403
    prompt = data.get('prompt','')
    changes = apply_prompt_logic(prompt)
    return jsonify({"changes": changes, "settings": app_settings})

@app.route('/api/bot_status')
def bot_status():
    return jsonify({"is_running": bot_running})

@app.route('/api/toggle_bot', methods=['POST'])
def toggle_bot():
    global bot_running
    bot_running = not bot_running
    return jsonify({"is_running": bot_running})

@app.route('/api/my_orders')
def my_orders():
    return jsonify({"orders": [], "platforms": app_settings["platforms"]})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
