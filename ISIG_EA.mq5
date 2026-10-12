//+------------------------------------------------------------------+
//| ISIG_EA.mq5  -  bridge between MetaTrader 5 and your bot app       |
//| 1) sends the closed candles of THIS chart to the app               |
//| 2) the app finds the setup and answers with an order               |
//| 3) this EA places the limit order with SL and TP attached          |
//| Lot size is calculated here from the risk amount the app sends.    |
//| v2: also reports the account type (demo/real), server and login so |
//|     the app can classify the account; obeys the app's poll timing. |
//+------------------------------------------------------------------+
#property copyright "Nnanna Michael"
#property version   "2.00"
#property description "Sends candles to the bot app and places the orders it returns."

#include <Trade\Trade.mqh>

input string InpServer      = "https://trading-bot-api-isig.onrender.com"; // Your app address (no slash at the end)
input string InpKey         = "";                                           // EA key shown when you saved the EA account
input int    InpSyncSeconds = 30;                                           // Start-up poll time; the app then tells the EA how often to call
input int    InpCandles     = 300;                                          // Closed candles sent to the app
input int    InpJitter      = 20;                                           // Random delay (seconds) after a bar closes, spreads the load on the app
input long   InpMagic       = 20260101;                                     // Magic number for orders placed by this EA

CTrade   trade;
string   g_placed[];        // order ids placed, waiting to be confirmed to the app
string   g_failed[];        // "id~reason" for orders the broker refused
datetime g_lastSent = 0;    // last closed bar already sent to the app
datetime g_jitterBar = 0;   // bar for which the random delay was drawn
datetime g_sendAfter = 0;   // do not send candles before this time
int      g_period = 0;      // current timer period in seconds
string   g_tier = "";       // what the app says about this account (demo / real)
string   g_msg  = "";       // message from the app (for example plan expired)

//+------------------------------------------------------------------+
string TfName()
  {
   switch(_Period)
     {
      case PERIOD_M1:  return "1m";
      case PERIOD_M5:  return "5m";
      case PERIOD_M15: return "15m";
      case PERIOD_M30: return "30m";
      case PERIOD_H1:  return "1h";
      case PERIOD_H4:  return "4h";
      case PERIOD_D1:  return "1d";
     }
   return "other";
  }

string Clean(string s)
  {
   StringReplace(s, "|", " ");
   StringReplace(s, "~", " ");
   StringReplace(s, ",", " ");
   StringReplace(s, "=", " ");
   StringReplace(s, "\n", " ");
   StringReplace(s, "\r", " ");
   return s;
  }

// account type exactly as MetaTrader reports it
string AccountMode()
  {
   switch((ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE))
     {
      case ACCOUNT_TRADE_MODE_DEMO:    return "DEMO";
      case ACCOUNT_TRADE_MODE_CONTEST: return "CONTEST";
      case ACCOUNT_TRADE_MODE_REAL:    return "REAL";
     }
   return "REAL";            // unknown is treated as real
  }

void SetPeriod(int sec)
  {
   sec = MathMax(5, MathMin(600, sec));
   if(sec == g_period) return;
   EventKillTimer();
   EventSetTimer(sec);
   g_period = sec;
  }

void AddUnique(string &arr[], string v)
  {
   for(int i = 0; i < ArraySize(arr); i++)
      if(arr[i] == v) return;
   int n = ArraySize(arr);
   ArrayResize(arr, n + 1);
   arr[n] = v;
  }

void Fail(string id, string err)
  {
   AddUnique(g_failed, id + "~" + Clean(err));
   Print("ISIG EA: order ", id, " not placed: ", err);
  }

//+------------------------------------------------------------------+
bool HaveOrderOrPosition(string comment)
  {
   for(int i = OrdersTotal() - 1; i >= 0; i--)
     {
      ulong tk = OrderGetTicket(i);
      if(tk > 0 && StringFind(OrderGetString(ORDER_COMMENT), comment) >= 0) return true;
     }
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong tk = PositionGetTicket(i);
      if(tk > 0 && StringFind(PositionGetString(POSITION_COMMENT), comment) >= 0) return true;
     }
   return false;
  }

void PlaceOrder(string id, string side, double entry, double sl, double tp, double risk)
  {
   string comment = "ISIG:" + id;
   if(HaveOrderOrPosition(comment)) { AddUnique(g_placed, id); return; }   // already placed earlier

   double tickSize = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   double tickVal  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE_LOSS);
   if(tickVal <= 0) tickVal = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double dist = MathAbs(entry - sl);
   if(tickSize <= 0 || tickVal <= 0 || dist <= 0) { Fail(id, "cannot size the trade"); return; }

   double lossPerLot = dist / tickSize * tickVal;               // money lost per 1.00 lot if the stop is hit
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double vmin = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double vmax = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   if(step <= 0) step = 0.01;
   double lots = MathFloor(risk / lossPerLot / step + 1e-9) * step;
   if(lots < vmin) { Fail(id, "lot below broker minimum " + DoubleToString(vmin, 2) + " (balance too small for this stop)"); return; }
   lots = MathMin(lots, vmax);
   int vd = (int)MathMax(0, MathRound(-MathLog10(step)));
   lots = NormalizeDouble(lots, vd);

   int dg = (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS);
   entry = NormalizeDouble(entry, dg);
   sl    = NormalizeDouble(sl, dg);
   tp    = NormalizeDouble(tp, dg);

   trade.SetExpertMagicNumber(InpMagic);
   bool ok;
   if(side == "BUY") ok = trade.BuyLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, comment);
   else               ok = trade.SellLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, comment);

   uint rc = trade.ResultRetcode();
   if(ok && (rc == TRADE_RETCODE_DONE || rc == TRADE_RETCODE_PLACED))
     {
      AddUnique(g_placed, id);
      Print("ISIG EA: placed ", side, " limit ", DoubleToString(lots, vd), " lots @ ", DoubleToString(entry, dg));
     }
   else
      Fail(id, "retcode " + IntegerToString((int)rc) + " " + trade.ResultRetcodeDescription());
  }

void CancelOrder(string id)
  {
   string comment = "ISIG:" + id;
   for(int i = OrdersTotal() - 1; i >= 0; i--)
     {
      ulong tk = OrderGetTicket(i);
      if(tk > 0 && StringFind(OrderGetString(ORDER_COMMENT), comment) >= 0)
        {
         trade.OrderDelete(tk);
         Print("ISIG EA: cancelled expired order ", id);
        }
     }
  }

//+------------------------------------------------------------------+
void ShowStatus(string status)
  {
   Comment("ISIG EA: ", status, "\n", _Symbol, " ", TfName(),
           (g_tier != "" ? "\nAccount: " + g_tier : ""),
           (g_msg  != "" ? "\n" + g_msg : ""),
           "\nlast check ", TimeToString(TimeLocal(), TIME_SECONDS));
  }

void ProcessResponse(string resp)
  {
   string lines[];
   int n = StringSplit(resp, '\n', lines);
   string status = "";
   g_msg = "";
   for(int i = 0; i < n; i++)
     {
      string ln = lines[i];
      StringTrimLeft(ln);
      StringTrimRight(ln);
      if(StringLen(ln) == 0) continue;
      string p[];
      int k = StringSplit(ln, '|', p);
      if(k < 1) continue;
      if(StringFind(ln, "OK ") == 0) status = "connected (" + StringSubstr(ln, 3) + " mode)";
      else if(p[0] == "NEXT" && k >= 2) SetPeriod((int)StringToInteger(p[1]));
      else if(p[0] == "TIER" && k >= 2) g_tier = StringToUpper(p[1]) + (k >= 3 ? " - " + p[2] : "");
      else if(p[0] == "MSG" && k >= 2) g_msg = p[1];
      else if(p[0] == "ORDER" && k >= 7)
         PlaceOrder(p[1], p[2], StringToDouble(p[3]), StringToDouble(p[4]), StringToDouble(p[5]), StringToDouble(p[6]));
      else if(p[0] == "CANCEL" && k >= 2)
         CancelOrder(p[1]);
     }
   ShowStatus(status);
  }

string BuildBody(bool withCandles)
  {
   string b = "KEY=" + InpKey + "\n";
   b += "SYMBOL=" + _Symbol + "\n";
   b += "TF=" + TfName() + "\n";
   b += "BAL=" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + "\n";
   // account facts straight from MetaTrader: the app uses them to decide demo or real
   b += "TRADEMODE=" + AccountMode() + "\n";
   b += "ACCSERVER=" + Clean(AccountInfoString(ACCOUNT_SERVER)) + "\n";
   b += "ACCCOMPANY=" + Clean(AccountInfoString(ACCOUNT_COMPANY)) + "\n";
   b += "ACCLOGIN=" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + "\n";
   b += "ACCCCY=" + Clean(AccountInfoString(ACCOUNT_CURRENCY)) + "\n";
   b += "EAVER=2\n";
   for(int i = 0; i < ArraySize(g_placed); i++) b += "PLACED=" + g_placed[i] + "\n";
   for(int i = 0; i < ArraySize(g_failed); i++) b += "FAILED=" + g_failed[i] + "\n";
   if(withCandles)
     {
      MqlRates r[];
      ArraySetAsSeries(r, false);                                   // oldest first
      int n = CopyRates(_Symbol, _Period, 1, InpCandles, r);        // start at 1 = skip the candle still forming
      if(n < 50) return "";
      b += "---\n";
      for(int i = 0; i < n; i++)
         b += IntegerToString((long)r[i].time) + "," + DoubleToString(r[i].open, _Digits) + "," + DoubleToString(r[i].high, _Digits) + ","
              + DoubleToString(r[i].low, _Digits) + "," + DoubleToString(r[i].close, _Digits) + "\n";
     }
   return b;
  }

void Sync()
  {
   if(StringLen(InpKey) < 10)
     {
      Comment("ISIG EA: paste your EA key into the EA inputs (F7 on the chart).");
      return;
     }
   datetime lastBar = iTime(_Symbol, _Period, 1);
   bool withC = false;
   if(lastBar != g_lastSent)                                        // a new closed bar: send candles, after a small random delay
     {
      if(g_jitterBar != lastBar)
        {
         g_jitterBar = lastBar;
         g_sendAfter = TimeLocal() + (MathRand() % (MathMax(0, InpJitter) + 1));
        }
      withC = (TimeLocal() >= g_sendAfter);
     }
   string body = BuildBody(withC);
   if(withC && body == "") { withC = false; body = BuildBody(false); }

   char data[], result[];
   string rh;
   StringToCharArray(body, data, 0, StringLen(body), CP_UTF8);
   ResetLastError();
   int code = WebRequest("POST", InpServer + "/ea/sync", "Content-Type: text/plain\r\n", 20000, data, result, rh);
   if(code == -1)
     {
      Comment("ISIG EA: cannot reach the app (error ", GetLastError(), ").\nAdd ", InpServer, " in Tools > Options > Expert Advisors > Allow WebRequest.");
      return;
     }
   string resp = CharArrayToString(result, 0, WHOLE_STRING, CP_UTF8);
   if(code != 200)
     {
      if(code == 429 || code == 503) SetPeriod(MathMax(g_period, 60));       // the app is busy: wait longer
      Comment("ISIG EA: ", resp);
      return;
     }
   if(withC) g_lastSent = lastBar;
   ArrayResize(g_placed, 0);                                        // the app received the confirmations
   ArrayResize(g_failed, 0);
   ProcessResponse(resp);
  }

//+------------------------------------------------------------------+
int OnInit()
  {
   trade.SetExpertMagicNumber(InpMagic);
   MathSrand((int)(GetTickCount() ^ (uint)AccountInfoInteger(ACCOUNT_LOGIN)));
   SetPeriod(InpSyncSeconds);
   Comment("ISIG EA: starting...");
   return INIT_SUCCEEDED;
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   Comment("");
  }

void OnTimer() { Sync(); }
void OnTick()  { }
