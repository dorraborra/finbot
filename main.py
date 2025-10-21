import os
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import closing
from typing import List

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, FSInputFile, BotCommand
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Переменная окружения BOT_TOKEN не задана")

LOCAL_TZ = timezone(timedelta(hours=0))
DB_PATH = os.getenv("DB_PATH", "finances.db")

CATEGORIES: List[str] = [
    "Сигареты","Кофе","Продукты","Ozon","WB",
    "Жрала не дома","Beauty","Бытовая химия","Такси","Квартира","Иное"
]

def db():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row; return conn

def init_db():
    with closing(db()) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS expenses("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER, amount REAL, category TEXT, created_at TEXT)"
        )

def inline_main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data="menu:add")
    kb.button(text="📊 Статистика", callback_data="menu:stats")
    kb.button(text="📁 Экспорт", callback_data="menu:export")
    kb.button(text="⚙️ Категории", callback_data="menu:cats")
    kb.adjust(2,2)
    return kb.as_markup()

def categories_kb():
    kb = ReplyKeyboardBuilder()
    for c in CATEGORIES: kb.button(text=c)
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=True)

def stats_inline_kb():
    kb = InlineKeyboardBuilder()
    for t,d in [("Сегодня","today"),("7 дней","7d"),("Месяц","month")]:
        kb.button(text=t, callback_data=f"stats:{d}")
    kb.adjust(3)
    return kb.as_markup()

class AddFlow(StatesGroup):
    waiting_amount = State()
    waiting_category = State()

def period_bounds(kind: str):
    now = datetime.now(tz=LOCAL_TZ)
    if kind == "today":
        start = datetime(now.year, now.month, now.day, tzinfo=LOCAL_TZ); end = start + timedelta(days=1); title = "Сегодня"
    elif kind == "7d":
        end = datetime(now.year, now.month, now.day, tzinfo=LOCAL_TZ) + timedelta(days=1); start = end - timedelta(days=7); title = "Последние 7 дней"
    else:
        start = datetime(now.year, now.month, 1, tzinfo=LOCAL_TZ)
        end = datetime(now.year + (1 if now.month == 12 else 0), 1 if now.month == 12 else now.month + 1, 1, tzinfo=LOCAL_TZ)
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
    total = sum(r["total"] or 0 for r in rows)
    return total, rows

router = Router()

WELCOME = ("✨ <b>Фин-бот</b>\nКидай сумму — спрошу категорию и всё запишу.\n\nВыбери действие:")

@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await message.answer(WELCOME, reply_markup=inline_main_menu(), parse_mode="HTML")
    await state.set_state(AddFlow.waiting_amount)

@router.message(Command("menu"))
async def menu_cmd(message: Message, state: FSMContext):
    await message.answer("Главное меню:", reply_markup=inline_main_menu())
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

@router.callback_query(F.data == "menu:cats")
async def cb_cats(cb: CallbackQuery):
    await cb.message.answer("Категории:\n• " + "\n• ".join(CATEGORIES))
    await cb.answer()

@router.message(AddFlow.waiting_amount, F.text.regexp(r"^\d+([.,]\d+)?$"))
async def got_amount(message: Message, state: FSMContext):
    amount = float(message.text.replace(",", "."))
    await state.update_data(amount=amount)
    await message.answer(f"Ок, <b>{amount:g}</b>. Выбери категорию:", reply_markup=categories_kb(), parse_mode="HTML")
    await state.set_state(AddFlow.waiting_category)

@router.message(AddFlow.waiting_amount)
async def must_number(message: Message):
    await message.answer("Отправь число, например: 390")

@router.message(AddFlow.waiting_category, F.text.in_(CATEGORIES))
async def got_category(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("amount")
    if amount is None:
        await message.answer("Сначала отправь сумму", reply_markup=inline_main_menu())
        await state.set_state(AddFlow.waiting_amount)
        return
    with closing(db()) as conn, conn:
        conn.execute("INSERT INTO expenses(user_id,amount,category,created_at) VALUES (?,?,?,?)",
                     (message.from_user.id, amount, message.text, datetime.now(tz=LOCAL_TZ).isoformat()))
    await message.answer(f"✅ Записала: <b>{amount:g}</b> • {message.text}", parse_mode="HTML", reply_markup=inline_main_menu())
    await state.clear()
    await state.set_state(AddFlow.waiting_amount)

@router.message(Command("stats"))
async def stats_cmd(message: Message):
    await message.answer("Выбери период:", reply_markup=stats_inline_kb())

@router.callback_query(F.data.startswith("stats:"))
async def stats_cb(cb: CallbackQuery):
    kind = cb.data.split(":", 1)[1]
    title, start, end = period_bounds(kind)
    total, rows = fetch_stats(cb.from_user.id, start, end)
    if not rows:
        await cb.message.answer(f"📊 {title}\nНет расходов", reply_markup=inline_main_menu())
    else:
        lines = [f"📊 <b>{title}</b>\nИтого: <b>{total:g}</b>"] + [f"• {r['category']}: {r['total']:g}" for r in rows]
        await cb.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=inline_main_menu())
    await cb.answer()

@router.message(Command("export"))
async def export_csv(message: Message):
    import os
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

async def main():
    init_db()
    bot = Bot(BOT_TOKEN)
    await bot.set_my_commands([
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="stats", description="Статистика"),
        BotCommand(command="export", description="Экспорт CSV"),
        BotCommand(command="start", description="Старт"),
    ])
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    print("Bot is running ✨")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
