"""
بات یکپارچه‌ی کانال (سروش پلاس): استخراج یوزرنیم اعضا + افزودن اعضا به کانال

روش کار:
  1) ادمین /start می‌زنه؛ اگه اکانت وصل نباشه، شماره و کد تأیید رو می‌فرسته (اکانت باید مالک/ادمین کانال باشه).
  2) از منو یکی رو انتخاب می‌کنه:
     📥 استخراج یوزرنیم‌ها : لینک کانال → همه‌ی @یوزرنیم‌ها به‌صورت متن + فایل txt.
        (لیست استخراج‌شده برای افزودن هم آماده می‌شه.)
     ➕ افزودن اعضا به کانال: لیست یوزرنیم‌ها (متن یا فایل) + لینک کانال مقصد → «شروع».
        سرعت خودکاره: از ADD_DELAY_MIN شروع می‌شه و با هر «صبر کن» سروش‌پلاس کند می‌شه.

⚠️ سروش‌پلاس افزودن انبوه رو محدود می‌کنه؛ کاربرانی که افزودن به کانال رو بسته باشن اضافه نمی‌شن
و سرعت زیاد ریسک محدود شدن اکانت رو بالا می‌بره.

env ها (همه اختیاری):
  BOT_TOKEN     : توکن بات
  ADMIN_ID      : آیدی عددی ادمین
  ADD_LIMIT     : حداکثر افزودن در هر بار اجرا (پیش‌فرض 1000)
  ADD_DELAY_MIN : فاصله‌ی شروع بین هر افزودن، ثانیه (پیش‌فرض 0.5)
  ADD_DELAY_MAX : سقف فاصله هنگام کند شدن، ثانیه (پیش‌فرض 20)
  DEEP_SEARCH   : 0 = جست‌وجوی حرف‌به‌حرف برای کانال‌های بزرگ خاموش (پیش‌فرض 1)
  INCLUDE_BOTS  : 1 = یوزرنیم بات‌ها هم تو استخراج باشه (پیش‌فرض 0)
"""
import os
import re
import io
import sys
import json
import time
import glob
import queue
import random
import getpass
import asyncio
import builtins
import inspect
import threading
import logging
import traceback
from collections import deque

import requests

TOKEN = os.getenv("BOT_TOKEN", "70542714:RI20_9ipAwoEohOYIhBKenfQCvuV4vNyzww").strip()
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "48985781").strip() or 0)
except ValueError:
    ADMIN_ID = 0


def _int_env(name, default):
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _float_env(name, default):
    try:
        return float(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


ADD_LIMIT = max(1, _int_env("ADD_LIMIT", 1000))
# فاصله‌ی شروع بین هر افزودن (ثانیه). سرعت خودکار تنظیم می‌شه: با هر «صبر کن» سروش‌پلاس دو برابر،
# و بعد از چند افزودنِ بی‌مشکل دوباره کمتر می‌شه. ADD_DELAY_MAX سقف فاصله‌ست.
ADD_DELAY_MIN = max(0.0, _float_env("ADD_DELAY_MIN", 0.5))
ADD_DELAY_MAX = max(ADD_DELAY_MIN, _float_env("ADD_DELAY_MAX", 20))
DEEP_SEARCH = os.getenv("DEEP_SEARCH", "1").strip() != "0"
INCLUDE_BOTS = os.getenv("INCLUDE_BOTS", "0").strip() == "1"

SESSION_NAME = "adder_session"
CONFIG_PATH = "adder_config.json"
CODE_TIMEOUT = 300
CHUNK_LIMIT = 3500
MAX_FILE_BYTES = 5 * 1024 * 1024

BASE_URL = "https://api.splus.ir/bot" + TOKEN
FILE_URLS = ["https://api.splus.ir/file/bot" + TOKEN + "/{path}",
             "https://api.splus.ir/bot" + TOKEN + "/file/{path}"]
SESSION = requests.Session()

try:
    import spluspy
    SPLUS_OK = True
except Exception as _e:  # pragma: no cover
    spluspy = None
    SPLUS_OK = False
    print("⚠️ کتابخونه‌ی spluspy نصب نیست:", _e)

try:
    from spluspy.tl import functions as tl_functions, types as tl_types
except Exception:  # pragma: no cover
    tl_functions, tl_types = None, None

S = {'gen': 0, 'running': False, 'starting': False, 'waiting': None, 'phone': None,
     'account': None, 'client': None, 'loop': None, 'state': None, 'busy': False,
     'stop': False, 'users': [], 'dest': None, 'flood_hits': 0, 'mode': None, 'picks': []}
ANSWERS = queue.Queue()

# لاگ هشدار/خطای کتابخونه (مثل RPC error ها) تو حافظه نگه داشته می‌شه تا آخر کار به‌صورت debug.txt برسه
LOG_BUF = deque(maxlen=400)


class _BufHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_BUF.append(f'{time.strftime("%H:%M:%S")} {record.name}: {record.getMessage()}')
        except Exception:
            pass


_root = logging.getLogger()
_root.setLevel(logging.WARNING)
_root.addHandler(_BufHandler(level=logging.WARNING))
_root.addHandler(logging.StreamHandler(sys.stderr))   # همچنان تو ترمینال هم چاپ بشه


def err_text(e):
    """متن کامل خطا: نوع + پیام + آخرِ traceback."""
    tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))[-900:]
    return f'{type(e).__name__}: {e!r}\n{tb}'


# ----------------------------- API سروش پلاس -----------------------------
def api_post(method, payload, timeout=15):
    try:
        return SESSION.post(f"{BASE_URL}/{method}", json=payload, timeout=timeout).json()
    except Exception as e:
        print(f"⚠️ {method} failed:", repr(e))
        return {'ok': False}


def send_message(chat_id, text, reply_markup=None):
    payload = {'chat_id': chat_id, 'text': text}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    return api_post('sendMessage', payload)


def delete_message(chat_id, message_id):
    api_post('deleteMessage', {'chat_id': chat_id, 'message_id': message_id}, timeout=8)


def send_document(chat_id, filename, data: bytes, caption=''):
    try:
        r = SESSION.post(f"{BASE_URL}/sendDocument", data={'chat_id': chat_id, 'caption': caption},
                         files={'document': (filename, io.BytesIO(data))}, timeout=60)
        return bool(r.json().get('ok'))
    except Exception as e:
        print("⚠️ sendDocument failed:", repr(e))
        return False


def get_updates(offset=None):
    params = {'timeout': 25}
    if offset:
        params['offset'] = offset
    try:
        return SESSION.get(f"{BASE_URL}/getUpdates", params=params, timeout=35).json()
    except Exception as e:
        print("⚠️ getUpdates:", repr(e))
        return {'ok': False, 'result': []}


def notify(text, reply_markup=None):
    send_message(ADMIN_ID, text, reply_markup)


CONTACT_KB = {'keyboard': [[{'text': '📱 ارسال شماره‌ی من', 'request_contact': True}]],
              'resize_keyboard': True, 'one_time_keyboard': True}
REMOVE_KB = {'remove_keyboard': True}


# ----------------------------- کمکی‌ها -----------------------------
def normalize_phone(s):
    d = re.sub(r'\D', '', str(s))
    if d.startswith('00'):
        d = d[2:]
    if d.startswith('0'):
        d = '98' + d[1:]
    elif len(d) == 10 and d.startswith('9'):
        d = '98' + d
    return '+' + d if len(d) >= 10 else None


def _invite_hash(s):
    m = re.search(r'(?:joinchat/|/\+|^\+)([A-Za-z0-9_\-]{8,})', s)
    return ('invite', m.group(1)) if m else None


def parse_channel(raw, allow_plain=True):
    """لینک/@یوزرنیم/آیدی عددی → یوزرنیم (str)، آیدی (int) یا ('invite', hash) برای لینک خصوصی؛ نامعتبر → None."""
    s = str(raw or '').strip()
    if not s or len(s) > 200 or '\n' in s:
        return None
    if 'joinchat' in s.lower() or re.search(r'/\+', s) or s.startswith('+'):
        return _invite_hash(s)
    if allow_plain and re.fullmatch(r'-?\d{5,}', s):
        return int(s)
    has_marker = s.startswith('@') or re.search(r'(splus\.ir|sapp\.ir|t\.me|telegram\.me)/', s, re.I) is not None
    if not has_marker and not allow_plain:
        return None
    name = s.split('?')[0].rstrip('/').split('/')[-1].lstrip('@')
    return name if re.fullmatch(r'[A-Za-z][\w.\-]{2,63}', name) else None


def chunk_lines(lines, limit=CHUNK_LIMIT):
    chunks, cur, size = [], [], 0
    for ln in lines:
        add = len(ln) + 1
        if cur and size + add > limit:
            chunks.append('\n'.join(cur))
            cur, size = [], 0
        cur.append(ln)
        size += add
    if cur:
        chunks.append('\n'.join(cur))
    return chunks


USERNAME_RE = re.compile(r'[A-Za-z][A-Za-z0-9_]{4,31}')


def parse_usernames(text):
    """متن/فایل → لیست یوزرنیم‌های یکتا (بدون @، به ترتیب ورود). اول @name ها؛ اگه نبود، هر توکنِ کامل."""
    text = str(text or '')
    found = re.findall(r'(?<![\w@])@([A-Za-z][A-Za-z0-9_]{4,31})(?![\w])', text)
    if not found:
        for tok in re.split(r'[\s,;،]+', text):
            tok = tok.strip().lstrip('@')
            if USERNAME_RE.fullmatch(tok):
                found.append(tok)
    seen, out = set(), []
    for n in found:
        if n.lower() not in seen:
            seen.add(n.lower())
            out.append(n)
    return out


def parse_dest(raw):
    """فقط لینک (splus.ir/... یا t.me/...) → یوزرنیم کانال؛ 'private' برای لینک دعوت؛ وگرنه None."""
    s = str(raw or '').strip()
    if not s or len(s) > 200 or '\n' in s:
        return None
    if re.search(r'(splus\.ir|sapp\.ir|t\.me|telegram\.me)/', s, re.I) is None:
        return None
    if 'joinchat' in s.lower() or re.search(r'/\+', s):
        return _invite_hash(s)
    name = s.split('?')[0].rstrip('/').split('/')[-1].lstrip('@')
    return name if re.fullmatch(r'[A-Za-z][\w.\-]{2,63}', name) else None


def download_file(file_id):
    """فایل ارسالی ادمین رو می‌گیره → bytes یا None."""
    info = api_post('getFile', {'file_id': file_id})
    path = ((info or {}).get('result') or {}).get('file_path')
    if not path:
        return None
    for tpl in FILE_URLS:
        try:
            r = SESSION.get(tpl.format(path=path), timeout=60)
            if r.status_code == 200 and r.content:
                return r.content[:MAX_FILE_BYTES]
        except Exception as e:
            print('⚠️ download failed:', repr(e))
    return None


# ----------------------------- افزودن اعضا (async) -----------------------------
def classify_error(e):
    r = (type(e).__name__ + ' ' + repr(e)).lower()
    if 'alreadyparticipant' in r or 'already' in r and 'participant' in r:
        return 'already'
    if 'privacy' in r or 'notmutual' in r or 'not_mutual' in r:
        return 'privacy'
    if 'userchannelstoomuch' in r or 'already in too many' in r:
        return 'user_busy'            # این کاربر عضو کانال‌های زیادیه؛ فقط خودش رد می‌شه
    if 'userstoomuch' in r or 'maximum number of users' in r or 'users_too_much' in r:
        return 'limit'                # کانال مقصد پر شده/به سقف افزودن رسیده؛ کل کار باید متوقف بشه
    if 'floodwait' in r or 'peerflood' in r or 'too_many' in r or 'toomany' in r or '429' in r or 'flood' in r:
        return 'flood'
    if 'adminrequired' in r or 'admin_required' in r or 'writeforbidden' in r or 'chatadmin' in r:
        return 'noadmin'
    if 'banned' in r or 'kicked' in r:
        return 'banned'
    if 'internal_server_error' in r or 'servererror' in r or 'rpcerror 500' in r or 'unsuccessful' in r:
        return 'server'
    if 'usernotoccupied' in r or 'usernameinvalid' in r or 'nosuchuser' in r or 'cannot find' in r or 'not found' in r:
        return 'not_found'
    return 'error'


def ref_label(ref):
    """نام قابل‌نمایش یه کانال (یوزرنیم، لینک خصوصی یا موجودیت انتخاب‌شده از /channels)."""
    if isinstance(ref, str):
        return ref
    if isinstance(ref, tuple):
        return 'کانال خصوصی'
    if isinstance(ref, int):
        return str(ref)
    return (getattr(ref, 'title', None) or getattr(ref, 'username', None) or str(getattr(ref, 'id', 'کانال')))


def _is_chan(e):
    return e is not None and (getattr(e, 'broadcast', False) or getattr(e, 'megagroup', False))


async def list_admin_channels(client, limit=60, admin_only=True):
    """کانال‌ها/گروه‌هایی که اکانت مالک یا ادمینشونه (عمومی و خصوصی)."""
    out = []
    try:
        if hasattr(client, 'iter_dialogs'):
            dialogs = []
            async for d in client.iter_dialogs():
                dialogs.append(d)
        else:
            dialogs = await client.get_dialogs()
        for d in dialogs:
            e = getattr(d, 'entity', None)
            if not _is_chan(e):
                continue
            if admin_only and not (getattr(e, 'creator', False) or getattr(e, 'admin_rights', None)):
                continue
            out.append(e)
            if len(out) >= limit:
                break
    except Exception as ex:
        print('[members] list channels error:', repr(ex))
    return out


async def get_channel_entity(client, ref):
    """ref: یوزرنیم (str)، آیدی عددی (int)، ('invite', hash) یا موجودیتِ آماده."""
    if isinstance(ref, tuple) and ref and ref[0] == 'invite':
        try:
            info = await client(tl_functions.messages.CheckChatInviteRequest(hash=ref[1]))
        except Exception as e:
            raise RuntimeError(f'لینک دعوت باز نشد ({e!r}). به‌جاش از /channels استفاده کن.')
        chat = getattr(info, 'chat', None)
        if chat is None:
            raise RuntimeError('اکانت عضو این کانال نیست؛ لینک دعوت فقط وقتی کار می‌کنه که اکانت قبلاً عضو/ادمین '
                               'کانال باشه. اکانت رو تو کانال اضافه کن یا از /channels استفاده کن.')
        return chat
    if isinstance(ref, int):
        try:
            return await client.get_entity(ref)
        except Exception:
            want = str(abs(ref))
            for e in await list_admin_channels(client, limit=1000, admin_only=False):
                eid = str(abs(getattr(e, 'id', 0)))
                if eid == want or want.endswith(eid):
                    return e
            raise RuntimeError('کانالی با این آیدی پیدا نشد. از /channels استفاده کن.')
    if isinstance(ref, str):
        getter = getattr(client, 'get_entity', None) or getattr(client, 'get_chat')
        return await getter(ref)
    return ref


async def resolve_dest(client, name):
    ch = await get_channel_entity(client, name)
    return await client.get_input_entity(ch)


async def add_one(client, dest_inp, username):
    """→ (کد نتیجه, توضیح)."""
    try:
        ent = await client.get_input_entity(username)
    except Exception as e:
        return 'not_found', err_text(e)
    res = None
    for attempt in range(2):
        try:
            res = await client(tl_functions.channels.InviteToChannelRequest(channel=dest_inp, users=[ent]))
            break
        except Exception as e:
            m = re.search(r'wait of (\d+) seconds', repr(e), re.I)
            if m:
                S['flood_hits'] += 1          # حلقه‌ی اصلی بر اساس این، سرعت رو کم می‌کنه
            if m and attempt == 0 and int(m.group(1)) <= 60:
                await asyncio.sleep(int(m.group(1)) + 1)   # صبر کوتاه سروش‌پلاس؛ یک بار دوباره امتحان
                continue
            if m:
                return 'flood', err_text(e)
            return classify_error(e), err_text(e)
    if getattr(res, 'missing_invitees', None):
        return 'privacy', 'missing_invitees'
    return 'added', ''


LABELS = {'added': '✅ اضافه شد', 'already': '↩️ از قبل عضو بود', 'privacy': '🔒 اجازه‌ی افزودن نداده',
          'not_found': '❓ پیدا نشد', 'banned': '🚫 از کانال محروم', 'error': '⚠️ خطای دیگه',
          'server': '🛠 خطای سرور سروش‌پلاس', 'flood': '⏳ محدودیت سرعت', 'noadmin': '🔑 دسترسی ادمین کافی نیست',
          'user_busy': '📚 عضو کانال‌های زیادیه', 'limit': '🧱 سقف افزودن کانال مقصد پر شده'}

STOP_REASONS = {
    'flood': 'محدودیت سرعت سروش‌پلاس — بعداً (چند ساعت دیگه) دوباره امتحان کن',
    'noadmin': 'اکانت باید ادمین کانال مقصد با اجازه‌ی افزودن عضو باشه',
    'limit': 'سروش‌پلاس می‌گه تعداد کاربرها به سقف رسیده («maximum number of users exceeded»). یعنی کانال مقصد '
             'دیگه افزودن دستی قبول نمی‌کنه. بقیه رو با لینک دعوت کانال برای خودشون بفرست یا بعداً دوباره امتحان کن.',
}


def run_add_job(dest_name, users):
    """تو ترد جدا اجرا می‌شه. اولین ADD_LIMIT نفر اضافه می‌شن؛ بقیه برای بار بعد برمی‌گردن."""
    results, stopped_reason = {}, None
    batch, rest = users[:ADD_LIMIT], users[ADD_LIMIT:]
    try:
        S['busy'], S['stop'] = True, False
        loop = S['loop']
        dest_inp = asyncio.run_coroutine_threadsafe(resolve_dest(S['client'], dest_name), loop).result(timeout=60)
        notify(f'🚀 شروع شد: {len(batch)} نفر → {ref_label(dest_name)}\n'
               f'⏱ سرعت خودکار: از {ADD_DELAY_MIN:g} ثانیه فاصله شروع می‌شه و با محدودیت سروش‌پلاس '
               f'کند می‌شه (تا {ADD_DELAY_MAX:g} ثانیه). برای توقف: /stop')
        cur, clean, last_hits, hard_fail = ADD_DELAY_MIN, 0, S['flood_hits'], 0
        for i, un in enumerate(batch, 1):
            if S['stop']:
                stopped_reason = 'توسط ادمین متوقف شد'
                rest = batch[i - 1:] + rest
                break
            code, info = asyncio.run_coroutine_threadsafe(add_one(S['client'], dest_inp, un), loop).result(timeout=120)
            results[un] = (code, info)
            if code not in ('added', 'already'):
                notify(f'❌ @{un} — {LABELS.get(code, code)}\n{info[:900]}')
            if code in STOP_REASONS:
                stopped_reason = LABELS[code] + ' — ' + STOP_REASONS[code]
                rest = batch[i - 1:] + rest
                results.pop(un, None)
                break
            # قطع‌کننده‌ی مدار: چند خطای پشت‌سرهم از نوع نامعلوم/سرور یعنی مشکل از کاربر نیست؛ ادامه ندیم
            hard_fail = hard_fail + 1 if code in ('error', 'server') else 0
            if hard_fail >= 5:
                stopped_reason = '۵ خطای پشت‌سرهم از سمت سرور/نامعلوم — برای جلوگیری از فشار الکی به اکانت متوقف شد'
                rest = batch[i:] + rest
                break
            if S['flood_hits'] != last_hits:       # سروش‌پلاس گفت «صبر کن»: کندتر شو
                last_hits, clean = S['flood_hits'], 0
                cur = min(ADD_DELAY_MAX, max(cur * 2, 2.0))
            elif code == 'added':
                clean += 1
                if clean >= 8:                      # چند بار بی‌مشکل: کمی تندتر شو
                    clean, cur = 0, max(ADD_DELAY_MIN, cur * 0.7)
            if i % 10 == 0:
                ok = sum(1 for c, _ in results.values() if c == 'added')
                notify(f'📊 {i}/{len(batch)} بررسی شد؛ {ok} نفر اضافه شد. (فاصله‌ی فعلی ≈ {cur:.1f} ثانیه)')
            if i < len(batch) and code == 'added':
                # فقط بعد از افزودنِ موفق صبر می‌کنیم؛ نفراتی که اضافه نشدن (خطا/حریم خصوصی) نیازی به فاصله ندارن
                time.sleep(random.uniform(cur * 0.8, cur * 1.2) if cur > 0 else 0)
    except Exception as e:
        print('[adder] job error:', repr(e))
        stopped_reason = f'خطا: {e!r}'
        notify(f'❌ خطا: {e!r}\nمطمئن شو لینک درسته و اکانت ({S["account"] or "؟"}) '
               f'ادمین کانال مقصد با اجازه‌ی افزودن عضوه.')
    finally:
        S['busy'] = False
    deliver_report(ref_label(dest_name), results, rest, stopped_reason)


def deliver_report(dest_name, results, rest, stopped_reason):
    counts = {}
    for code, _ in results.values():
        counts[code] = counts.get(code, 0) + 1
    lines = [f'📋 گزارش افزودن به {dest_name}']
    if stopped_reason:
        lines.append(f'⛔ متوقف شد: {stopped_reason}')
    for code in ('added', 'already', 'privacy', 'not_found', 'user_busy', 'banned', 'server', 'error'):
        if counts.get(code):
            lines.append(f'{LABELS[code]}: {counts[code]:,}')
    if counts.get('server') or counts.get('error'):
        first = next((i for c, i in results.values() if c in ('server', 'error') and i), '')
        if first:
            lines.append(f'ℹ️ نمونه‌ی خطا: {first.splitlines()[0][:200]}')
    if rest:
        lines.append(f'⏭ باقی‌مونده برای بار بعد: {len(rest):,}')
    send_message(ADMIN_ID, '\n'.join(lines))

    detail = []
    for code in ('privacy', 'not_found', 'user_busy', 'banned', 'server', 'error'):
        items = [(u, info) for u, (c, info) in results.items() if c == code]
        if items:
            detail.append(f'--- {LABELS[code]} ({len(items)}) ---')
            for u, info in items:
                detail.append(f'@{u}')
                if info:
                    detail.append('    ' + info.strip().replace('\n', '\n    '))
    if detail:
        send_document(ADMIN_ID, 'failed.txt', '\n'.join(detail).encode('utf-8'), caption='نفرات اضافه‌نشده')
    if LOG_BUF and (detail or stopped_reason):
        send_document(ADMIN_ID, 'debug.txt', '\n'.join(LOG_BUF).encode('utf-8'),
                      caption='لاگ خطاها/هشدارهای کتابخونه (برای رفع باگ)')
    if rest:
        body = ['@' + u for u in rest]
        for part in chunk_lines(body):
            send_message(ADMIN_ID, part)
            time.sleep(0.4)
        send_document(ADMIN_ID, 'remaining.txt', '\n'.join(body).encode('utf-8'),
                      caption='باقی‌مونده؛ برای ادامه همین لیست رو دوباره بفرست')
    S['users'] = list(rest)                       # باقی‌مونده‌ها برای بار بعد آماده می‌مونن
    S['state'] = 'users' if S['running'] else None
    if rest:
        send_message(ADMIN_ID, f'🔁 {len(rest):,} نفر باقی‌مونده آماده‌ست؛ هر وقت خواستی دوباره لینک مقصد رو بفرست.')


BTN_EXTRACT = '📥 استخراج یوزرنیم‌ها'
BTN_ADD = '➕ افزودن اعضا به کانال'
MENU_KB = {'keyboard': [[{'text': BTN_EXTRACT}, {'text': BTN_ADD}]], 'resize_keyboard': True}


def username_of(user):
    """یوزرنیم فعال یه کاربر (بدون @) یا None."""
    un = getattr(user, 'username', None)
    if un:
        return str(un)
    for item in (getattr(user, 'usernames', None) or []):
        if getattr(item, 'active', True) and getattr(item, 'username', None):
            return str(item.username)
    return None


def part_uid(pt):
    uid = getattr(pt, 'user_id', None)
    if uid is None:
        uid = getattr(getattr(pt, 'peer', None), 'user_id', None)
    return uid


def is_member(pt):
    if tl_types is None:
        return True
    return not isinstance(pt, (tl_types.ChannelParticipantLeft, tl_types.ChannelParticipantBanned))


# ----------------------------- استخراج اعضا (async) -----------------------------
async def _collect(client, inp, mk, found, errors):
    offset = 0
    while offset < 50000:
        try:
            res = await client(tl_functions.channels.GetParticipantsRequest(
                channel=inp, filter=mk(), offset=offset, limit=200, hash=0))
        except Exception as e:
            errors.append(repr(e))
            print('[members] GetParticipants error:', repr(e))
            return
        parts = getattr(res, 'participants', None) or []
        if not parts:
            return
        by_id = {getattr(u, 'id', None): u for u in (getattr(res, 'users', None) or [])}
        for pt in parts:
            uid = part_uid(pt)
            if uid is None or not is_member(pt):
                continue
            usr = by_id.get(uid)
            if usr is not None or uid not in found:
                found[uid] = usr if usr is not None else found.get(uid)
        offset += len(parts)
        if len(parts) < 200:
            return


async def fetch_members(client, name, progress=None):
    """→ (found: {user_id: user|None}, total_in_channel|None). خطا → Exception."""
    ch = await get_channel_entity(client, name)
    inp = await client.get_input_entity(ch)

    found, errors = {}, []
    makers = (lambda: tl_types.ChannelParticipantsRecent(),
              lambda: tl_types.ChannelParticipantsSearch(q=''),
              lambda: tl_types.ChannelParticipantsAdmins())
    await asyncio.gather(*[_collect(client, inp, mk, found, errors) for mk in makers])

    if not found:  # روش خام جواب نداد؛ متد آماده‌ی کتابخونه
        try:
            if hasattr(client, 'iter_participants'):
                async for p in client.iter_participants(ch):
                    found[p.id] = p
            elif hasattr(client, 'get_participants'):
                for p in await client.get_participants(ch):
                    found[p.id] = p
        except Exception as e:
            errors.append(repr(e))
    if not found and errors:
        raise RuntimeError(errors[0])

    total = None
    try:
        full = await client(tl_functions.channels.GetFullChannelRequest(inp))
        total = getattr(getattr(full, 'full_chat', None), 'participants_count', None)
    except Exception as e:
        print('[members] GetFullChannel error:', repr(e))

    if DEEP_SEARCH and total and total > len(found):
        if progress:
            progress(f'🔎 {len(found):,} از {total:,} عضو پیدا شد؛ جست‌وجوی حرف‌به‌حرف برای بقیه ...')
        letters = 'abcdefghijklmnopqrstuvwxyz0123456789_' + 'ابپتثجچحخدذرزسشصضطظعغفقکگلمنوهی'
        for i, q in enumerate(letters):
            if len(found) >= total:
                break
            try:
                res = await client(tl_functions.channels.GetParticipantsRequest(
                    channel=inp, filter=tl_types.ChannelParticipantsSearch(q=q),
                    offset=0, limit=200, hash=0))
            except Exception as e:
                print('[members] deep search error:', repr(e))
                break
            by_id = {getattr(u, 'id', None): u for u in (getattr(res, 'users', None) or [])}
            for pt in (getattr(res, 'participants', None) or []):
                uid = part_uid(pt)
                if uid is not None and is_member(pt):
                    found[uid] = by_id.get(uid) or found.get(uid)
            await asyncio.sleep(0.25)
    return found, total


def build_report(found, total):
    """→ (usernames[@...], آمار)."""
    names, no_username, bots, deleted = set(), 0, 0, 0
    for uid, usr in found.items():
        if usr is None:
            no_username += 1
            continue
        if getattr(usr, 'deleted', False):
            deleted += 1
            continue
        if getattr(usr, 'bot', False) and not INCLUDE_BOTS:
            bots += 1
            continue
        un = username_of(usr)
        if un:
            names.add('@' + un)
        else:
            no_username += 1
    out = sorted(names, key=str.lower)
    return out, {'members': len(found), 'total': total, 'with_username': len(out),
                 'no_username': no_username, 'bots': bots, 'deleted': deleted}


def deliver(chat_id, name, usernames, st):
    head = (f'✅ کانال: {name}\n'
            f'👥 اعضای خوانده‌شده: {st["members"]:,}'
            + (f' (از {st["total"]:,} عضو کانال)' if st['total'] else '') + '\n'
            f'🆔 دارای یوزرنیم: {st["with_username"]:,}\n'
            f'🚫 بدون یوزرنیم: {st["no_username"]:,}'
            + (f'\n🤖 بات‌ها (حذف شد): {st["bots"]:,}' if st['bots'] else '')
            + (f'\n👻 حذف‌شده: {st["deleted"]:,}' if st['deleted'] else ''))
    if st['total'] and st['members'] < st['total']:
        head += ('\n\n⚠️ تعداد خوانده‌شده کمتر از اعضای کانالِ. سروش پلاس لیست اعضای کانال‌های بزرگ رو '
                 'کامل نمی‌ده یا بعضی اعضا مخفی‌ان.')
    send_message(chat_id, head)
    if not usernames:
        send_message(chat_id, 'هیچ یوزرنیمی پیدا نشد.')
        return
    for part in chunk_lines(usernames):
        send_message(chat_id, part)
        time.sleep(0.4)
    ok = send_document(chat_id, f'{re.sub(r"[^A-Za-z0-9_.-]", "_", str(name))[:40] or "channel"}_usernames.txt', '\n'.join(usernames).encode('utf-8'),
                       caption=f'{len(usernames):,} یوزرنیم')
    if not ok:
        send_message(chat_id, '⚠️ ارسال فایل txt انجام نشد (احتمالاً سروش پلاس فایل رو قبول نکرد)؛ لیست بالا به‌صورت متن کامله.')


def run_extract_job(name):
    """تو ترد جدا اجرا می‌شه تا حلقه‌ی بات قفل نشه."""
    try:
        S['busy'] = True
        label = ref_label(name)
        notify(f'⏳ در حال خوندن اعضای {label} ... (برای کانال‌های بزرگ چند دقیقه طول می‌کشه)')
        fut = asyncio.run_coroutine_threadsafe(
            fetch_members(S['client'], name, progress=notify), S['loop'])
        found, total = fut.result(timeout=1800)
        usernames, st = build_report(found, total)
        deliver(ADMIN_ID, label, usernames, st)
        S['users'] = [u[1:] for u in usernames]       # برای «افزودن اعضا» آماده بمونه
        notify(f'✅ {len(usernames):,} یوزرنیم برای افزودن آماده شد (دکمه‌ی «{BTN_ADD}»).\n'
               '🔗 اگه کانال دیگه‌ای می‌خوای بخونی، لینکش رو بفرست.')
    except Exception as e:
        print('[members] job error:', repr(e))
        notify(f'❌ نتونستم اعضای کانال رو بخونم.\nمطمئن شو لینک درسته و اکانت ({S["account"] or "؟"}) '
               f'مالک/ادمین کانال هست.\nخطا: {e!r}')
    finally:
        S['busy'] = False


# ----------------------------- لاگین اکانت (userbot) -----------------------------
def ask_admin(kind):
    while not ANSWERS.empty():
        ANSWERS.get_nowait()
    S['waiting'] = kind
    if kind == 'code':
        notify('📩 کد تأیید به سروش پلاسِ اکانتت ارسال شد.\n'
               'کد رو همین‌جا بفرست، ولی با خط‌تیره بین رقم‌ها، مثلاً 1-2-3-4-5\n'
               '(اگه سرراست بفرستی ممکنه سروش پلاس باطلش کنه)')
    else:
        notify('🔐 اکانتت رمز دومرحله‌ای داره. رمزت رو بفرست:')
    try:
        ans = ANSWERS.get(timeout=CODE_TIMEOUT)
    except queue.Empty:
        S['waiting'] = None
        raise TimeoutError('از ادمین جوابی نیومد')
    S['waiting'] = None
    return re.sub(r'\D', '', ans) if kind == 'code' else ans


def _fake_input(prompt=''):
    p = str(prompt).lower()
    if any(k in p for k in ('phone', 'mobile', 'number', 'شماره')):
        return S['phone'] or ''
    if any(k in p for k in ('pass', '2fa', 'رمز')):
        return ask_admin('password')
    return ask_admin('code')


if SPLUS_OK:
    builtins.input = _fake_input
    getpass.getpass = lambda prompt='', stream=None: _fake_input(prompt)


def cfg_load():
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def cfg_save(cfg):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False)


def session_files():
    return [f for f in glob.glob(SESSION_NAME + '*')
            if not f.endswith(('-journal', '-wal', '-shm')) and os.path.isfile(f)]


async def _userbot_main(phone, gen):
    S['loop'] = asyncio.get_running_loop()
    client = spluspy.Client(SESSION_NAME)
    kwargs = {'phone': phone}
    try:
        if 'code_callback' in inspect.signature(client.start).parameters:
            kwargs['code_callback'] = lambda: ask_admin('code')
    except (TypeError, ValueError):
        pass
    res = client.start(**kwargs)
    if inspect.isawaitable(res):
        await res

    cfg_save({'phone': phone})
    try:
        me = client.get_me()
        if inspect.isawaitable(me):
            me = await me
        un = getattr(me, 'username', None)
        S['account'] = ('@' + un) if un else str(getattr(me, 'id', ''))
    except Exception as e:
        print('[members] get_me error:', repr(e))

    if S['gen'] != gen:
        return
    S.update(running=True, starting=False, client=client, state='menu', mode=None)
    send_message(ADMIN_ID, '✅ لاگین انجام شد.\nیکی از گزینه‌ها رو انتخاب کن:', MENU_KB)
    while S['gen'] == gen:
        await asyncio.sleep(1)
    res = client.disconnect()
    if inspect.isawaitable(res):
        await res


def _userbot_thread(phone, gen):
    try:
        asyncio.run(_userbot_main(phone, gen))
    except Exception as e:
        print('[members] userbot error:', repr(e))
        if S['gen'] == gen:
            S.update(running=False, starting=False, waiting=None, state=None)
            notify(f'❌ خطا در اتصال اکانت:\n{e!r}\n\nدوباره /start بزن.')


def start_userbot(phone):
    S['gen'] += 1
    S.update(phone=phone, starting=True, running=False, client=None)
    threading.Thread(target=_userbot_thread, args=(phone, S['gen']), daemon=True).start()


def logout():
    S['gen'] += 1
    S.update(running=False, starting=False, waiting=None, client=None, state=None, mode=None, account=None)
    time.sleep(1.5)
    for f in session_files():
        try:
            os.remove(f)
        except OSError:
            pass
    try:
        os.remove(CONFIG_PATH)
    except OSError:
        pass


# ----------------------------- پیام‌های ادمین -----------------------------
def show_menu(text='یکی از گزینه‌ها رو انتخاب کن:'):
    S.update(mode=None, state='menu')
    send_message(ADMIN_ID, text, MENU_KB)


def enter_extract():
    S.update(mode='extract', state='extract')
    send_message(ADMIN_ID, '🔗 لینک یا @یوزرنیم کانال رو بفرست.\n'
                           '🔒 برای کانال خصوصی: لینک دعوت (https://splus.ir/+...) رو بفرست، یا /channels بزن '
                           'تا لیست کانال‌های اکانت رو ببینی و با شماره انتخاب کنی.\n'
                           '(اکانت وصل‌شده باید مالک/ادمین اون کانال باشه)\nبرگشت به منو: /menu', MENU_KB)


def enter_add():
    S.update(mode='add', state='users', dest=None)
    n = len(S['users'])
    if n:
        send_message(ADMIN_ID, f'📋 {n:,} یوزرنیم آماده‌ست (از آخرین استخراج/لیست قبلی).\n\n'
                               '🔗 لینک کانال مقصد رو بفرست (https://splus.ir/... یا لینک دعوت خصوصی)\n'
                               'یا /channels بزن تا از بین کانال‌های اکانت با شماره انتخاب کنی.\n'
                               'یوزرنیم‌های بیشتر/لیست جدید هم می‌تونی بفرستی. برای خالی کردن لیست: /cancel', MENU_KB)
    else:
        send_message(ADMIN_ID, '📋 لیست یوزرنیم‌ها رو به‌صورت متن (@user1 @user2 ...) یا فایل txt بفرست.\n'
                               'می‌تونی چند پیام پشت‌سرهم بفرستی؛ همه جمع می‌شن.\nبرگشت به منو: /menu', MENU_KB)


def add_users(chat_id, names):
    if not names:
        send_message(chat_id, '❌ هیچ یوزرنیمی تو این پیام/فایل پیدا نشد.')
        return
    have = {u.lower() for u in S['users']}
    new = [n for n in names if n.lower() not in have]
    S['users'].extend(new)
    S['state'] = 'users'
    send_message(chat_id, f'✅ {len(new):,} یوزرنیم جدید اضافه شد (جمع: {len(S["users"]):,}).\n\n'
                          f'🔗 حالا لینک کانال مقصد رو بفرست (https://splus.ir/...)\n'
                          f'یا اگه یوزرنیم‌های بیشتری هست بفرست.')


def show_channels():
    """لیست کانال‌های مالک/ادمین اکانت (تو ترد جدا، تا حلقه‌ی بات قفل نشه)."""
    try:
        ents = asyncio.run_coroutine_threadsafe(list_admin_channels(S['client']), S['loop']).result(timeout=90)
    except Exception as e:
        notify(f'❌ نتونستم لیست کانال‌ها رو بگیرم: {e!r}')
        return
    if not ents:
        notify('کانالی پیدا نشد که اکانت مالک/ادمینش باشه. اگه کانال خصوصی‌ه، لینک دعوتش رو بفرست.')
        return
    S['picks'], S['state'] = ents, 'pick'
    lines = []
    for i, e in enumerate(ents, 1):
        un = getattr(e, 'username', None)
        lines.append(f'{i}) {getattr(e, "title", None) or "بدون نام"} — ' + (f'@{un}' if un else '🔒 خصوصی'))
    for part in chunk_lines(lines):
        send_message(ADMIN_ID, part)
    send_message(ADMIN_ID, '👆 شماره‌ی کانال مورد نظر رو بفرست. برای لغو: /menu')


def set_dest_and_confirm(chat_id, dest):
    if not S['users']:
        send_message(chat_id, 'اول لیست یوزرنیم‌ها رو بفرست.')
        return
    S['dest'], S['state'] = dest, 'confirm'
    n = len(S['users'])
    send_message(chat_id, f'📌 مقصد: {ref_label(dest)}\n👥 تعداد یوزرنیم: {n:,}\n'
                          f'این بار حداکثر {min(n, ADD_LIMIT):,} نفر اضافه می‌شه'
                          + (f' و {n - ADD_LIMIT:,} نفر برای بار بعد می‌مونه' if n > ADD_LIMIT else '')
                          + '.\n\nبرای شروع بنویس: شروع\nبرای لغو: /cancel')


def handle_message(m):
    chat_id = m.get('chat', {}).get('id')
    if chat_id != ADMIN_ID:
        return
    text = (m.get('text') or '').strip()
    contact = m.get('contact')
    doc = m.get('document')
    cmd = text.split()[0].split('@')[0].lower() if text.startswith('/') else ''

    if S['waiting'] and text and not cmd:
        ANSWERS.put(text)
        if m.get('message_id') is not None:
            delete_message(chat_id, m['message_id'])
        return

    if cmd == '/logout':
        logout()
        send_message(chat_id, '🗑 اکانت قطع شد و سشن پاک شد. برای وصل‌شدن دوباره /start بزن.', REMOVE_KB)
        return
    if cmd == '/stop':
        S['stop'] = True
        send_message(chat_id, '⏹ بعد از افزودن فعلی متوقف می‌شه ...')
        return
    if cmd == '/status':
        st = '🟢 وصل' if S['running'] else ('🟡 در حال وصل‌شدن' if S['starting'] else '🔴 وصل نیست')
        send_message(chat_id, f'وضعیت اکانت: {st}\nاکانت: {S["account"] or "—"}\n'
                              f'لیست آماده: {len(S["users"]):,} یوزرنیم\nمقصد: {ref_label(S["dest"]) if S["dest"] else "—"}')
        return
    if cmd == '/cancel':
        S.update(users=[], dest=None)
        if S['running']:
            show_menu('باشه، لیست پاک شد.')
        else:
            S['state'] = None
            send_message(chat_id, 'باشه، لغو شد.', REMOVE_KB)
        return

    if cmd == '/start':
        if not SPLUS_OK:
            send_message(chat_id, '⚠️ کتابخونه‌ی spluspy روی سرور نصب نیست.')
        elif S['running']:
            show_menu()
        elif S['starting']:
            send_message(chat_id, '⏳ اکانت در حال وصل‌شدنه. اگه کد اومد، با خط‌تیره بفرست.')
        else:
            S['state'] = 'phone'
            send_message(chat_id, '📱 شماره‌ی اکانتی رو بفرست که مالک/ادمین کانال هست '
                                  '(با دکمه‌ی زیر یا دستی).\nبرای لغو /cancel', CONTACT_KB)
        return

    if S['state'] == 'phone' and (contact or (text and not cmd)):
        phone = normalize_phone((contact or {}).get('phone_number') or text)
        if not phone:
            send_message(chat_id, '❌ شماره معتبر نیست. مثل 09123456789 بفرست.')
            return
        S['state'] = None
        send_message(chat_id, f'⏳ در حال اتصال با {phone} ... کد تأیید برات می‌آد.', REMOVE_KB)
        start_userbot(phone)
        return

    if not S['running']:
        if text and not cmd:
            send_message(chat_id, 'اول /start بزن تا اکانتت رو وصل کنیم.')
        return
    if S['busy']:
        if text and not cmd:
            send_message(chat_id, '⏳ یه عملیات در حال انجامه. برای توقف /stop')
        return

    # منو
    if cmd == '/menu':
        show_menu()
        return
    if text == BTN_EXTRACT or cmd == '/extract':
        enter_extract()
        return
    if text == BTN_ADD or cmd == '/add':
        enter_add()
        return
    if S['mode'] is None:
        if text or doc:
            show_menu()
        return
    if cmd == '/channels':
        threading.Thread(target=show_channels, daemon=True).start()
        return
    if S['state'] == 'pick' and text.isdigit():
        idx = int(text) - 1
        if 0 <= idx < len(S['picks']):
            ref = S['picks'][idx]
            if S['mode'] == 'extract':
                S['state'] = 'extract'
                threading.Thread(target=run_extract_job, args=(ref,), daemon=True).start()
            else:
                set_dest_and_confirm(chat_id, ref)
        else:
            send_message(chat_id, '❌ شماره معتبر نیست.')
        return

    # ---- حالت استخراج ----
    if S['mode'] == 'extract':
        if text and not cmd:
            name = parse_channel(text, allow_plain=True)
            if name is None:
                send_message(chat_id, '❌ لینک یا یوزرنیم کانال معتبر نیست. مثل https://splus.ir/mychannel، '
                                      '@mychannel یا لینک دعوت خصوصی. یا /channels بزن.')
            else:
                threading.Thread(target=run_extract_job, args=(name,), daemon=True).start()
        return

    # ---- حالت افزودن ----
    if doc:
        data = download_file(doc.get('file_id'))
        if not data:
            send_message(chat_id, '❌ نتونستم فایل رو بگیرم. لیست رو به‌صورت متن بفرست.')
            return
        add_users(chat_id, parse_usernames(data.decode('utf-8-sig', errors='ignore')))
        return

    if text and not cmd:
        dest = parse_dest(text)
        if dest:
            set_dest_and_confirm(chat_id, dest)
            return
        if S['state'] == 'confirm' and text.strip().lower() in ('شروع', 'start', 'go'):
            threading.Thread(target=run_add_job, args=(S['dest'], list(S['users'])), daemon=True).start()
            return
        names = parse_usernames(text)
        if names:
            add_users(chat_id, names)
        else:
            send_message(chat_id, 'یوزرنیمی پیدا نکردم. مثل @user1 @user2 بفرست، یا لینک کانال مقصد.')


def main():
    if not TOKEN:
        sys.exit('متغیر BOT_TOKEN تنظیم نشده.')
    if not ADMIN_ID:
        sys.exit('متغیر ADMIN_ID تنظیم نشده.')
    api_post('deleteWebhook', {'drop_pending_updates': False}, timeout=10)
    api_post('setMyCommands', {'commands': [
        {'command': 'start', 'description': 'شروع / منو'},
        {'command': 'extract', 'description': 'استخراج یوزرنیم اعضای کانال'},
        {'command': 'add', 'description': 'افزودن اعضا به کانال'},
        {'command': 'channels', 'description': 'لیست کانال‌های اکانت (برای کانال خصوصی)'},
        {'command': 'stop', 'description': 'توقف افزودن'},
        {'command': 'status', 'description': 'وضعیت'},
        {'command': 'logout', 'description': 'قطع اکانت'},
        {'command': 'cancel', 'description': 'پاک‌کردن لیست'}]}, timeout=10)

    phone = cfg_load().get('phone')
    if SPLUS_OK and phone and session_files():
        start_userbot(phone)
    print('✅ بات بالا اومد')

    offset = None
    while True:
        data = get_updates(offset)
        updates = data.get('result') or []
        for up in updates:
            offset = up.get('update_id', 0) + 1
            m = up.get('message')
            if m:
                try:
                    handle_message(m)
                except Exception as e:
                    print('⚠️ handler error:', repr(e))
        if not updates and not data.get('ok'):
            time.sleep(2)


if __name__ == '__main__':
    main()
