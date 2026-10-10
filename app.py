from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# FREE storage
accounts = {}

@app.route('/')
def dashboard():
    total = len(accounts)
    auth = sum(1 for a in accounts.values() if a.get('authorized'))
    running = sum(1 for a in accounts.values() if a.get('bot_running'))
    html = f"""
    <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
    <style>
    body{{background:#0a0a0a;color:white;font-family:Arial;padding:20px}}
   .card{{background:#1a1a1a;border:1px solid #333;border-radius:12px;padding:20px;margin:10px 0}}
   .green{{color:#00ff88}}.stat{{font-size:28px;font-weight:bold}}
    input{{width:100%;padding:12px;margin:5px 0;border-radius:8px;border:1px solid #333;background:#222;color:white}}
    button{{width:100%;padding:14px;background:#6c5ce7;color:white;border:none;border-radius:10px;font-size:16px;margin-top:10px}}
   .account{{background:#222;padding:15px;border-radius:10px;margin:10px 0;border-left:4px solid #6c5ce7}}
    </style></head><body>
    <h2>Account Control Dashboard</h2>
    <div style='display:flex;gap:10px'>
      <div class='card'><div>REGISTERED</div><div class='stat'>{total} total</div></div>
      <div class='card'><div>AUTHORIZED</div><div class='stat green'>{auth} accounts</div></div>
      <div class='card'><div>BOTS RUNNING</div><div class='stat green'>{running} active</div></div>
    </div>
    <div style='color:#00ff88'>API connected - FREE FOREVER</div>
    <div class='card'>
      <h3>Register an account</h3>
      <form action='/register' method='post'>
        <input name='account' placeholder='Account number (e.g. 8472015)' required>
        <input name='broker' placeholder='Broker name (e.g. Deriv)' required>
        <button type='submit'>+ Register & Authorize</button>
      </form>
    </div>
    <h3>Your Accounts:</h3>
    """
    for acc_id, data in accounts.items():
        html += f"<div class='account'><b>Account: {acc_id}</b> - {data['broker']}<br>"
        html += f"Authorized: YES | Bot: {'RUNNING' if data['bot_running'] else 'STOPPED'}<br>"
        html += f"<a href='/toggle/{acc_id}' style='color:#6c5ce7'>Toggle Bot</a> | <a href='/delete/{acc_id}' style='color:red'>Delete</a></div>"
    html += "</body></html>"
    return html

@app.route('/register', methods=['POST'])
def register():
    acc = request.form.get('account','').strip()
    broker = request.form.get('broker','').strip()
    if acc:
        accounts[acc] = {'broker': broker, 'authorized': True, 'bot_running': True}
    return dashboard()

@app.route('/toggle/<acc_id>')
def toggle(acc_id):
    if acc_id in accounts:
        accounts[acc_id]['bot_running'] = not accounts[acc_id]['bot_running']
    return dashboard()

@app.route('/delete/<acc_id>')
def delete(acc_id):
    if acc_id in accounts:
        del accounts[acc_id]
    return dashboard()

@app.route('/api/is_allowed')
def is_allowed():
    acc = request.args.get('account','')
    allowed = accounts.get(acc, {}).get('authorized', False)
    return jsonify({'allowed': allowed})

@app.route('/api/bot_status')
def bot_status():
    acc = request.args.get('account','')
    running = accounts.get(acc, {}).get('bot_running', False)
    return jsonify({'bot_running': running})

@app.route('/api/accounts')
def list_accounts():
    return jsonify(accounts)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
