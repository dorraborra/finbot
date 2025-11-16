import os
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import closing
from typing import List, Tuple
import random

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, FSInputFile, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramNetworkError

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

LOCAL_TZ = timezone(timedelta(hours=0))
DB_PATH = os.getenv("DB_PATH", "finances.db")

# ---------- Категории ----------
CATEGORY_OPTIONS: List[Tuple[str, str]] = [
    ("🚬 Сигареты", "Сигареты"),
    ("☕ Кофе", "Кофе"),
    ("🛒 Продукты", "Продукты"),
    ("📦 Ozon", "Ozon"),
    ("🛍 WB", "WB"),
    ("🍔 Было лень готовить", "Жрала не дома"),
    ("💄 Beauty", "Beauty"),
    ("🧽 Бытовая химия", "Бытовая химия"),
    ("🚕 Такси", "Такси"),
    ("🏠 Квартира", "Квартира"),
    ("⛽ Бензин", "Бензин"),
    ("🧼 Мойка", "Мойка"),
    ("🏢 Офис", "Офис"),
    ("💪 Спортзал", "Спортзал"),
    ("📁 Иное", "Иное"),
]
RAW_CATEGORIES: List[str] = [r for _, r in CATEGORY_OPTIONS]
LABEL_BY_RAW = {raw: label for (label, raw) in CATEGORY_OPTIONS}

# ---------- Цитаты по категориям ----------
CATEGORY_QUOTES = {
    "Сигареты": [
        "🚭 Каждый шаг — в сторону лёгкого дыхания 💗",
        "🫶 Мягкое напоминание: здоровье всегда важнее.",
        "✨ Фламинго верит: однажды ты скажешь сигаретам «бай» 💋",
        "🩵 Ты сильнее, чем думаешь.",
    ],
    "Кофе": [
        "☕ Немножко кофе — немножко магии ✨",
        "🌸 Кофе — это объятия в кружке.",
        "💖 Заряжаемся и двигаемся дальше!",
        "🦩 Кофейная богиня выходит на арену.",
    ],
    "Продукты": [
        "🛒 Продуктовая миссия выполнена 💫",
        "🍓 Дом станет вкуснее!",
        "✨ Ты управляешь своим холодильником как королева.",
        "💗 Планирование — это тоже забота о себе.",
    ],
    "Ozon": [
        "📦 Интернет-шоппинг — твоё второе имя 💙",
        "💫 Посылочка — это маленькое счастье!",
        "✨ Ты знаешь толк в удобстве.",
        "🦩 Фламинго одобряет! Почти инвестиции 😎",
    ],
    "WB": [
        "🛍 Стиль, удобство и немного магии ✨",
        "💜 Уверена, это была лучшая находка дня.",
        "💅 Fashion mode: ON.",
        "🌸 Ты делаешь жизнь красивее!",
    ],
    "Жрала не дома": [
        "🍔 Быстро, вкусно и без стресса 💖",
        "✨ Иногда готовить — слишком переоценено.",
        "💗 Ты выбрала себя, и это прекрасно!",
        "🦩 Вкусняшка записана, идём дальше.",
    ],
    "Beauty": [
        "💄 Красота начинается с любви к себе ✨",
        "🌸 Ты — эстетика и стиль.",
        "💖 Инвестиции в себя — самые приятные.",
        "🧚‍♀️ Ты сияешь всё ярче!",
    ],
    "Бытовая химия": [
        "🧽 Чистый дом — чистая голова ✨",
        "🌼 Маленькие шаги к уюту.",
        "💗 Ты создаёшь пространство, в котором приятно жить.",
        "🏡 Дом улыбается!",
    ],
    "Такси": [
        "🚕 Комфорт — это важно ✨",
        "🌙 Иногда лучше доехать красиво.",
        "💛 Быстро, удобно, без заморочек.",
        "🦩 Леди так не ходят — они ездят 😉",
    ],
    "Квартира": [
        "🏠 Дом — это место силы 💗",
        "✨ Ты вкладываешься в уют и стабильность.",
        "🌸 Красиво жить — тоже искусство.",
        "🩵 Уютный мир, созданный тобой.",
    ],
    "Бензин": [
        "⛽ Дорога ждёт 🌙",
        "🚗 С полным баком жизнь приятнее.",
        "✨ Ты управляешь маршрутом своей жизни.",
        "🦩 Фламинго бы тоже заправился, если бы мог.",
    ],
    "Мойка": [
        "🧼 Чистая машина — чистая аура ✨",
        "🚘 Красотка блестит!",
        "💙 Новенькая, будто с салона.",
        "🌟 Прямо как ты — сияет.",
    ],
    "Офис": [
        "🏢 Дела делаются, успехи растут ✨",
        "📈 Ты — продуктивность в человеческом виде.",
        "💼 Фламинго видит: ты справляешься отлично.",
        "💡 Важный вклад в большое будущее.",
    ],
    "Спортзал": [
        "💪 Ты сила. Ты нежность. Ты баланс ✨",
        "🔥 Тело говорит «спасибо»!",
        "🦩 Один шаг ближе к идеальной себе.",
        "💖 Ты сделала больше, чем ничего.",
    ],
    "Иное": [
        "✨ Записано. На шаг ближе к прозрачности и порядку 💗",
        "🦩 Маленький контроль — большое спокойствие.",
        "🌸 Ты становишься лучше, чем вчера.",
        "💖 Продолжай в том же духе.",
    ],
}

# ---------- База данных ----------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS expenses("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER, amount REAL, category TEXT, created_at TEXT)"
        )

# ---------- UNDO ----------
def undo_last_expense(user_id: int):
    with closing(db()) as conn, conn:
        row = conn.execute(
            "SELECT id, amount, category, created_at "
            "FROM expenses WHERE user_id=? "
            "ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if not row:
            return None
        conn.execute("DELETE FROM expenses WHERE id=?", (row["id"],))
        return row

# ---------- Клавиатуры ----------
def categories_kb(page: int = 0, per_row: int = 2, page_size: int = 10):
    start = page * page_size
    end = start + page_size
    slice_ = CATEGORY_OPTIONS[start:end]

    kb = InlineKeyboardBuilder()
    for idx, (label, _) in enumerate(slice_, start=start):
        kb.button(text=label, callback_data=f"pick:{idx}")
    kb.adjust(per_row)

    pages = (len(CATEGORY_OPTIONS) + page_size - 1) // page_size
    if pages > 1:
        nav = InlineKeyboardBuilder()
        if page > 0:
            nav.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
        nav.button(text=f"Стр. {page+1}/{pages}", callback_data="noop")
        if page < pages - 1:
            nav.button(text="Вперёд ➡️", callback_data=f"page:{page+1}")
        nav.adjust(3)
        kb.row(*nav.buttons)

    return kb.as_markup()

def inline_main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data="menu:add")
    kb.button(text="📊 Статистика", callback_data="menu:stats")
    kb.button(text="📁 Экспорт CSV", callback_data="menu:export")
    kb.button(text="ℹ️ Помощь", callback_data="menu:help")
    kb.button(text="↩️ Отменить", callback_data="menu:undo")
    kb.button(text="🧹 Сбросить мои данные", callback_data="menu:reset")
    kb.adjust(2, 2, 2)
    return kb.as_markup()

def stats_inline_kb():
    kb = InlineKeyboardBuilder()
    for t, d in [("Сегодня", "today"), ("7 дней", "7d"), ("Месяц", "month")]:
        kb.button(text=t, callback_data=f"stats:{d}")
    kb.adjust(3)
    return kb.as_markup()

# ---------- FSM ----------
class AddFlow(StatesGroup):
    waiting_amount = State()
    waiting_category = State()

router = Router()

WELCOME = (
    "🦩 <b>Flamingo Money</b>\n"
    "Твой лёгкий учёт расходов: кидай сумму — я спрошу категорию и всё запишу.\n\n"
    "💡 Что умею:\n"
    "• 📊 Статистика по дням/неделям/месяцу\n"
    "• 📁 Экспорт в CSV одним нажатием\n"
    "• ➕ Быстрое добавление трат\n\n"
    "Выбери действие ниже:"
)

# ---------- Вспомогательные ----------
def period_bounds(kind: str):
    now = datetime.now(tz=LOCAL_TZ)
    if kind == "today":
        start = datetime(now.year, now.month, now.day, tzinfo=LOCAL_TZ)
        end = start + timedelta(days=1)
        title = "Сегодня"
    elif kind == "7d":
        end = datetime(now.year, now.month, now.day, tzinfo=LOCAL_TZ) + timedelta(days=1)
        start = end - timedelta(days=7)
        title = "Последние 7 дней"
    else:
        start = datetime(now.year, now.month, 1, tzinfo=LOCAL_TZ)
        end = datetime(
            now.year + (1 if now.month == 12 else 0),
            1 if now.month == 12 else now.month + 1,
            1,
            tzinfo=LOCAL_TZ,
        )
        title = "Текущий месяц"
    return title, start, end

def fetch_stats(user_id: int, start: datetime, end: datetime):
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT category, SUM(amount) AS total FROM expenses "
            "WHERE user_id=? AND created_at>=? AND created_at<? "
            "GROUP BY category ORDER BY total DESC",
            (user_id, start.isoformat(), end.isoformat()),
        ).fetchall()
    total = sum((r["total"] or 0) for r in rows)
    return total, rows

def bar(value: float, max_value: float, width: int = 14) -> str:
    if max_value <= 0:
        return "░" * width
    filled = int(round((value / max_value) * width))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)

# ---------- Профиль /me ----------
def get_user_profile(user_id: int):
    with closing(db()) as conn:
        row_total = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS t FROM expenses WHERE user_id=?",
            (user_id,),
        ).fetchone()
        total = float(row_total["t"] or 0)

        row_top = conn.execute(
            "SELECT category, SUM(amount) AS s FROM expenses "
            "WHERE user_id=? GROUP BY category ORDER BY s DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if row_top:
            raw = row_top["category"]
            top_category = LABEL_BY_RAW.get(raw, raw)
        else:
            top_category = "—"

        row_days = conn.execute(
            "SELECT COUNT(DISTINCT date(created_at)) AS d FROM expenses WHERE user_id=?",
            (user_id,),
        ).fetchone()
        days_total = int(row_days["d"] or 0)

        dates = conn.execute(
            "SELECT DISTINCT date(created_at) AS d "
            "FROM expenses WHERE user_id=? AND created_at>=? "
            "ORDER BY d DESC",
            (user_id, (datetime.now(tz=LOCAL_TZ) - timedelta(days=120)).isoformat()),
        ).fetchall()

    today = datetime.now(tz=LOCAL_TZ).date()
    date_set = {
        datetime.fromisoformat(r["d"]).date()
        if "T" in r["d"]
        else datetime.strptime(r["d"], "%Y-%m-%d").date()
        for r in dates
    }
    streak = 0
    cur = today
    while cur in date_set:
        streak += 1
        cur = cur - timedelta(days=1)

    avg_per_day = round(total / days_total, 2) if days_total else 0.0
    with closing(db()) as conn:
        row_30 = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS t FROM expenses "
            "WHERE user_id=? AND created_at>=?",
            (user_id, (datetime.now(tz=LOCAL_TZ) - timedelta(days=30)).isoformat()),
        ).fetchone()
    last30 = float(row_30["t"] or 0)
    avg_30 = round(last30 / 30, 2)

    return {
        "total": total,
        "top_category": top_category,
        "days_total": days_total,
        "streak": streak,
        "avg_per_day": avg_per_day,
        "avg_30": avg_30,
    }

# ---------- Хэндлеры ----------
@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await message.answer(WELCOME, reply_markup=inline_main_menu(), parse_mode="HTML")
    await state.set_state(AddFlow.waiting_amount)

@router.message(Command("menu"))
async def menu_cmd(message: Message, state: FSMContext):
    await message.answer("🧭 Главное меню:", reply_markup=inline_main_menu())
    await state.set_state(AddFlow.waiting_amount)

@router.callback_query(F.data == "menu:add")
async def cb_add(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введи сумму (например: <b>390</b>)", parse_mode="HTML")
    await state.set_state(AddFlow.waiting_amount)
    await cb.answer()

@router.callback_query(F.data == "menu:stats")
async def cb_stats(cb: CallbackQuery):
    await cb.message.answer("Выбери период:", reply_markup=stats_inline_kb())
    await cb.answer()

@router.callback_query(F.data == "menu:export")
async def cb_export(cb: CallbackQuery):
    await export_csv(cb.message)
    await cb.answer()

@router.callback_query(F.data == "menu:help")
async def cb_help(cb: CallbackQuery):
    text = (
        "ℹ️ <b>Как пользоваться</b>\n\n"
        "1) Отправь число — это сумма траты (например: <b>390</b>).\n"
        "2) Выбери категорию из списка.\n"
        "3) Готово! Запись попадёт в статистику и экспорт.\n\n"
        "Команды:\n"
        "• /menu — главное меню\n"
        "• /stats — выбор периода статистики\n"
        "• /export — выгрузка CSV\n"
        "• /reset_me — удалить только свои траты\n"
        "• /undo — отменить последнюю трату\n"
        "• /me — мой профиль\n"
        "• /start — перезапуск приветствия"
    )
    await cb.message.answer(text, parse_mode="HTML", reply_markup=inline_main_menu())
    await cb.answer()

@router.callback_query(F.data == "menu:reset")
async def menu_reset(cb: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Да, удалить только мои траты", callback_data="myreset:confirm")
    kb.button(text="Отмена", callback_data="myreset:cancel")
    kb.adjust(1)
    await cb.message.answer(
        "⚠️ Уверена, что хочешь удалить все свои записи?\n"
        "Это действие <b>нельзя отменить</b>.",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
    )
    await cb.answer()

@router.callback_query(F.data == "myreset:cancel")
async def myreset_cancel(cb: CallbackQuery):
    await cb.message.answer("Отменено ✅", reply_markup=inline_main_menu())
    await cb.answer()

@router.callback_query(F.data == "myreset:confirm")
async def myreset_confirm(cb: CallbackQuery):
    with closing(db()) as conn, conn:
        conn.execute("DELETE FROM expenses WHERE user_id=?", (cb.from_user.id,))
    await cb.message.answer("🧹 Готово! Все твои траты удалены.", reply_markup=inline_main_menu())
    await cb.answer()

# -------- UNDO: кнопка в меню --------
@router.callback_query(F.data == "menu:undo")
async def menu_undo(cb: CallbackQuery):
    row = undo_last_expense(cb.from_user.id)
    if not row:
        await cb.message.answer("😌 У тебя пока нет записей, нечего отменять.", reply_markup=inline_main_menu())
    else:
        label = LABEL_BY_RAW.get(row["category"], row["category"])
        amount = row["amount"]
        txt = (
            "↩️ <b>Отменила последнюю запись</b>\n\n"
            f"{amount:g} • {label}\n\n"
            "💡 Можно отправить новую сумму — запишу следующую трату."
        )
        await cb.message.answer(txt, parse_mode="HTML", reply_markup=inline_main_menu())
    await cb.answer()

# -------- UNDO: команда /undo --------
@router.message(Command("undo"))
async def undo_cmd(message: Message):
    row = undo_last_expense(message.from_user.id)
    if not row:
        await message.answer("😌 У тебя пока нет записей, нечего отменять.", reply_markup=inline_main_menu())
    else:
        label = LABEL_BY_RAW.get(row["category"], row["category"])
        amount = row["amount"]
        txt = (
            "↩️ <b>Отменила последнюю запись</b>\n\n"
            f"{amount:g} • {label}\n\n"
            "Можешь продолжать — отправь следующую сумму 💖"
        )
        await message.answer(txt, parse_mode="HTML", reply_markup=inline_main_menu())

# -------- /me ----------
@router.message(Command("me"))
async def me_cmd(message: Message):
    p = get_user_profile(message.from_user.id)
    compliments = [
        "🦩 Ты ведёшь учёт как настоящая фламинго-икона 💖",
        "💅 Финансы под контролем — ты буквально богиня бюджета ✨",
        "🌸 Стильно, точно, без Excel-страданий 💕",
        "🩵 Финансовый дзен достигнут, можно кофе ☕",
    ]
    msg = (
        f"<b>🦩 Твой профиль Flamingo</b>\n\n"
        f"💰 <b>Всего потрачено:</b> {p['total']:.2f}\n"
        f"💫 <b>Любимая категория:</b> {p['top_category']}\n"
        f"📅 <b>Дней с записями:</b> {p['days_total']}\n"
        f"🔥 <b>Текущая серия:</b> {p['streak']} дней подряд\n"
        f"➗ <b>Среднее/день:</b> {p['avg_per_day']:.2f}\n"
        f"📆 <b>За 30 дней в среднем/день:</b> {p['avg_30']:.2f}\n\n"
        f"{random.choice(compliments)}"
    )
    await message.answer(msg, parse_mode="HTML", reply_markup=inline_main_menu())

# -------- Добавление трат --------
@router.message(AddFlow.waiting_amount, F.text.regexp(r"^\d+([.,]\d+)?$"))
async def got_amount(message: Message, state: FSMContext):
    amount = float(message.text.replace(",", "."))
    await state.update_data(amount=amount)
    await message.answer(
        f"Ок, <b>{amount:g}</b>. Теперь выбери категорию:",
        parse_mode="HTML",
        reply_markup=categories_kb(page=0)
    )
    await state.set_state(AddFlow.waiting_category)

@router.message(AddFlow.waiting_amount)
async def must_number(message: Message):
    await message.answer("Отправь число, например: 390")

@router.callback_query(F.data.startswith("page:"))
async def page_cb(cb: CallbackQuery):
    page = int(cb.data.split(":", 1)[1])
    await cb.message.edit_reply_markup(reply_markup=categories_kb(page=page))
    await cb.answer()

@router.callback_query(F.data == "noop")
async def noop_cb(cb: CallbackQuery):
    await cb.answer()

@router.callback_query(AddFlow.waiting_category, F.data.startswith("pick:"))
async def picked_category(cb: CallbackQuery, state: FSMContext):
    idx = int(cb.data.split(":", 1)[1])
    label, raw = CATEGORY_OPTIONS[idx]
    data = await state.get_data()
    amount = data.get("amount")
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO expenses(user_id,amount,category,created_at) VALUES (?,?,?,?)",
            (cb.from_user.id, amount, raw, datetime.now(tz=LOCAL_TZ).isoformat()),
        )

    main_text = (
        "✅ <b>Записала трату</b>\n\n"
        f"{amount:g} • {label}\n\n"
        "💡 Что дальше:\n"
        "• отправь ещё сумму — добавлю следующую трату\n"
        "• /stats — посмотреть статистику\n"
        "• /undo — отменить последнюю запись\n"
    )
    await cb.message.answer(
        main_text,
        parse_mode="HTML",
        reply_markup=inline_main_menu(),
    )

    # тематическая цитата по категории
    try:
        quotes = CATEGORY_QUOTES.get(raw, CATEGORY_QUOTES["Иное"])
        quote = random.choice(quotes)
        await cb.message.answer(quote)
    except Exception:
        pass

    await state.clear()
    await state.set_state(AddFlow.waiting_amount)
    await cb.answer()

def build_stats_text(title: str, total: float, rows):
    max_val = max((r["total"] or 0) for r in rows) or 1.0
    lines = [f"📊 <b>{title}</b>\nИтого: <b>{total:g}</b>\n"]
    for r in rows:
        raw = r["category"]
        lbl = LABEL_BY_RAW.get(raw, raw)
        val = float(r["total"] or 0)
        lines.append(f"{lbl} — {val:g}\n{bar(val, max_val)}")
    return "\n".join(lines)

@router.callback_query(F.data.startswith("stats:"))
async def stats_cb(cb: CallbackQuery):
    kind = cb.data.split(":", 1)[1]
    title, start, end = period_bounds(kind)
    total, rows = fetch_stats(cb.from_user.id, start, end)
    if not rows:
        await cb.message.answer(f"📊 {title}\nНет расходов", reply_markup=inline_main_menu())
    else:
        await cb.message.answer(build_stats_text(title, total, rows), parse_mode="HTML", reply_markup=inline_main_menu())
    await cb.answer()

@router.message(Command("export"))
async def export_csv(message: Message):
    os.makedirs("exports", exist_ok=True)
    path = f"exports/{message.from_user.id}_export.csv"
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT amount, category, created_at FROM expenses WHERE user_id=? ORDER BY created_at DESC",
            (message.from_user.id,),
        ).fetchall()
    with open(path, "w", encoding="utf-8") as f:
        f.write("amount;category;created_at\n")
        for r in rows:
            f.write(f"{r['amount']};{r['category']};{r['created_at']}\n")
    await message.answer_document(FSInputFile(path), caption="📁 CSV экспорт")

# ---------- Команды с ретраями ----------
async def set_commands_with_retry(bot: Bot):
    cmds = [
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="stats", description="Статистика"),
        BotCommand(command="export", description="Экспорт CSV"),
        BotCommand(command="undo", description="Отменить последнюю трату"),
        BotCommand(command="me", description="Мой профиль 🦩"),
        BotCommand(command="start", description="Старт"),
    ]
    for attempt in range(3):
        try:
            await bot.set_my_commands(cmds, request_timeout=30)
            return
        except TelegramNetworkError:
            wait = 2 * (attempt + 1)
            print(f"[set_my_commands] timeout, retry in {wait}s… ({attempt+1}/3)")
            await asyncio.sleep(wait)
    print("[set_my_commands] gave up after retries; continue without crashing")

# ---------- Точка входа ----------
async def main():
    init_db()
    bot = Bot(BOT_TOKEN)
    await set_commands_with_retry(bot)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    print("Bot is running ✨")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
